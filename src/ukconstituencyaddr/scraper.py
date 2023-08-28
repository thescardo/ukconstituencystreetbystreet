import argparse
from contextlib import contextmanager
from datetime import datetime
import json
from collections import defaultdict, deque
from dataclasses import dataclass
import multiprocessing
import re
import threading
import time
from typing import Deque, Dict, List, Optional, Set, Tuple
import logging
from sqlalchemy import select, update
import tqdm
from urllib3.util.retry import Retry
import concurrent.futures
import difflib

import requests
from requests.status_codes import codes
import requests.adapters
from sqlalchemy.orm import Session

from ukconstituencyaddr.db import db_repr_sqlite as db_repr
from ukconstituencyaddr import config

HOUSE_NUMBER_PATTERN = re.compile(
    r"^(\d+[a-zA-Z]{0,1}\s{0,1}[-]{0,1}\s{0,1}\d*[a-zA-Z]{0,1})\s+(.*)$"
)
LTD_PO_BOX_PATTERN = re.compile(
    r'.*(ltd|po box).*', re.IGNORECASE
)

PARTIAL_LOOKUP_MAX_NUM_ADDRESSES = 20

MAX_FULL_ADDRESS_LOOKUPS_PER_DAY = 5000

TEMPLATE = (
    "{line_1}|{line_2}|{line_3}|{line_4}|{town_or_city}|{locality}|{county}|{country}"
)

GET_ADDRESS_IO_SUGGESTIONS_KEY = "suggestions"
GET_ADDRESS_IO_ADDRESS_KEY = "address"
GET_ADDRESS_IO_ID_KEY = "id"

FIRST_20_ADDR_LOOKUP_DATA_FIELD = {
    "all": False,
    "top": PARTIAL_LOOKUP_MAX_NUM_ADDRESSES,
    "template": TEMPLATE,
}

ALL_RESULTS = {
    "all": True,
    "template": TEMPLATE,
}


def retry_session(
    retries: int = 20,
    session: Optional[requests.Session] = None,
    backoff_factor: float = 2.0,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        respect_retry_after_header=True,
        status_forcelist=[429],
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_address_resp_for_postcode(
    postcode: str,
    full_lookup: bool,
    session: Optional[requests.Session] = None,
) -> Tuple[requests.Response, Optional[Dict[str, Dict]]]:
    if len(postcode) < 5:
        raise ValueError("Postcodes must be 5 characters or longer")

    if session is None:
        session = retry_session()

    api_key = config.config.scraping.get_address_io_api_key

    url = f"https://api.getAddress.io/autocomplete/{postcode}?api-key={api_key}"
    headers = {"content-type": "application/json"}
    if full_lookup:
        params = ALL_RESULTS
    else:
        params = FIRST_20_ADDR_LOOKUP_DATA_FIELD

    current = 10
    while True:
        response = session.get(url=url, headers=headers, params=params)

        match response.status_code:
            case 200:
                return response, json.loads(response.text)
            case 429:
                time.sleep(current)
                current *= 1.5
                continue
            case _:
                return response, None


@dataclass
class UsageCounts:
    UsageToday: int
    DailyLimit: int
    MonthlyBuffer: int
    MonthlyBufferUsed: int


def get_limit_for_day(session: Optional[requests.Session] = None) -> UsageCounts:
    if session is None:
        session = retry_session()

    DEFAULT_USAGE = UsageCounts(0, 5000, 500, 0)

    api_key = config.config.scraping.get_address_io_api_key.strip()
    if len(api_key) == 0:
        # Just assume values
        return DEFAULT_USAGE

    time_now = datetime.now()
    url = f"https://api.getAddress.io/v3/usage/{time_now.day}/{time_now.month}/{time_now.year}?api-key={api_key}"
    response = session.get(url=url)

    match response.status_code:
        case 200:
            parsed = json.loads(response.text)
            return UsageCounts(
                UsageToday=parsed["usage_today"],
                DailyLimit=parsed["daily_limit"],
                MonthlyBuffer=parsed["monthly_buffer"],
                MonthlyBufferUsed=["monthly_buffer_used"],
            )
        case 403:
            return DEFAULT_USAGE
        case _:
            response.raise_for_status()


@contextmanager
def acquire_lock_timeout(lock: threading.Lock, timeout: float):
    result = lock.acquire(timeout=timeout)
    try:
        yield result
    finally:
        if result:
            lock.release()


class NumAddressReqManager:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.Lock()

        self._num_req_remaining = MAX_FULL_ADDRESS_LOOKUPS_PER_DAY
        self._last_date = datetime.now()

        self._usages: UsageCounts = get_limit_for_day()
        self.logger.info(f"Usages limits are: {self._usages}")

    def request_and_decrement(self) -> bool:
        with acquire_lock_timeout(self._lock, timeout=5) as locked:
            if locked:
                # Check if we need to reset the number of lookups
                time_now = datetime.now()
                if self._last_date.date() != time_now.date():
                    self._usages = get_limit_for_day()

                if self._usages.UsageToday < self._usages.DailyLimit:
                    self._usages.UsageToday += 1
                    return True
                elif self._usages.MonthlyBufferUsed < self._usages.MonthlyBuffer:
                    self._usages.MonthlyBufferUsed += 1
                    return True
                else:
                    return False
            else:
                raise TimeoutError("Failed to acquire lock within timeout")


class Scraper:
    def __init__(self) -> None:
        self.engine = db_repr.get_engine()
        db_repr.Base.metadata.create_all(bind=self.engine)

        self.logger = logging.getLogger(self.__class__.__name__)

        self.num_req_manger = NumAddressReqManager()
        self._db_lock = threading.Lock()

        self.max_simultaneous_loops = 20
        self.use_full_lookups = True

        self.streets_per_postcode_district: Dict[str, Set[str]] = defaultdict(set)

    def get_addresses_for_postcode(
        self,
        ons_postcode: db_repr.OnsPostcode,
        http_sess: Optional[requests.Session] = None,
    ) -> Tuple[bool, bool]:
        if http_sess is None:
            http_sess = retry_session(backoff_factor=10)

        postcode = ons_postcode.postcode
        address_deque: Deque[db_repr.SimpleAddress] = deque()

        with Session(self.engine) as db_sess:
            fetched = (
                db_sess.query(db_repr.PostcodeFetched)
                .where(db_repr.PostcodeFetched.postcode == postcode)
                .one_or_none()
            )

        if fetched is not None and fetched.was_fetched:
            self.logger.info(f"Already fetched addresses {postcode=}")
            return 200, True
        self.logger.info(f"Fetching addresses for {postcode=}")

        resp, parsed = get_address_resp_for_postcode(
            postcode=postcode, full_lookup=False, session=http_sess
        )

        match resp.status_code:
            case 200:
                pass
            case 429, 503:
                time.sleep(5)
                return True, False
            case _:
                self.logger.error(f"Unhandled reason code {resp.reason}")
                return True, False

        self.logger.debug(f"Got {len(parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY])=}")

        num_addresses = len(parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY])
        if num_addresses == PARTIAL_LOOKUP_MAX_NUM_ADDRESSES:
            if not self.use_full_lookups:
                self.logger.debug(
                    f"{self.use_full_lookups=} Not getting "
                    f"all addresses for {postcode=}"
                )
                return False, False

            if self.num_req_manger.request_and_decrement():
                self.logger.debug(f"Need to get all addresses for {postcode=}")
                resp, parsed = get_address_resp_for_postcode(
                    postcode=postcode, full_lookup=True, session=http_sess
                )

                match resp.status_code:
                    case 200:
                        pass
                    case 429, 503:
                        time.sleep(5)
                        return True, False
                    case _:
                        self.logger.error(f"Unhandled reason code {resp.reason}")
                        return True, False
            else:
                self.logger.debug("NOT")
                return False, False

        self.logger.debug(
            f"Got {len(parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY])} addresses: {parsed}"
        )

        for item in parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY]:
            if GET_ADDRESS_IO_ID_KEY in item and GET_ADDRESS_IO_ADDRESS_KEY in item:
                get_address_io_id = item[GET_ADDRESS_IO_ID_KEY]
                line_list = item[GET_ADDRESS_IO_ADDRESS_KEY].split("|")

                address_deque.append(
                    db_repr.SimpleAddress(
                        postcode=postcode,
                        line_1=line_list[0],
                        line_2=line_list[1],
                        line_3=line_list[2],
                        line_4=line_list[3],
                        house_num_or_name="",
                        thoroughfare_or_desc="",
                        town_or_city=line_list[4],
                        locality=line_list[5],
                        county=line_list[6],
                        country=line_list[7],
                        get_address_io_id=get_address_io_id,
                    )
                )

        with Session(self.engine) as db_sess:
            with self._db_lock:
                db_sess.add_all(address_deque)
                db_sess.add(
                    db_repr.PostcodeFetched(
                        postcode=postcode,
                        constituency_id=ons_postcode.constituency_id,
                        was_fetched=True,
                    )
                )
                db_sess.commit()
                return True, True

    def scrape_for_constituency(self, constituency_name: str):
        with Session(self.engine) as session:
            results = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == constituency_name)
                .all()
            )
            if len(results) == 0:
                raise ReferenceError("Need to have parsed ONS files to scrape data")

            process = tqdm.tqdm(
                total=len(results),
                desc=f"Fetching addresses for postcodes in {constituency_name}",
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                # Start the load operations and mark each future with its URL
                get_fut = [
                    executor.submit(
                        self.get_addresses_for_postcode, ons_postcode=postcode
                    )
                    for postcode in results
                ]

                for _ in concurrent.futures.as_completed(get_fut):
                    process.update(1)

    def scrape(self, constituencies_to_scrape: List[str]):
        self.logger.info(f"Scraping addresses for {constituencies_to_scrape}")

        with Session(self.engine) as session:
            start_num_addresses = session.query(db_repr.SimpleAddress).count()

        for constituency_name in constituencies_to_scrape:
            self.scrape_for_constituency(constituency_name)

        with Session(self.engine) as session:
            end_num_addresses = session.query(db_repr.SimpleAddress).count()

        new_addresses = end_num_addresses - start_num_addresses
        self.logger.info(f"Scraped {new_addresses} new addresses")

    def process_thoroughfares_for_postcode(self, ons_postcode: db_repr.OnsPostcode):
        with Session(self.engine) as session:
            session.add(ons_postcode)

            addresses = ons_postcode.addresses

            if ons_postcode.postcode_district not in self.streets_per_postcode_district:
                os_roads = session.query(db_repr.OsOpennameRoad).where(db_repr.OsOpennameRoad.postcode_district == ons_postcode.postcode_district).all()

                roads = self.streets_per_postcode_district[ons_postcode.postcode_district]
                for os_road in os_roads:
                    roads.add(os_road.name)
            else:
                roads = self.streets_per_postcode_district[ons_postcode.postcode_district]

            not_found_1st: Deque[db_repr.SimpleAddress] = deque()
            road_names_found: Set[str] = set()

            # First pass using difflib
            for address in addresses:
                if len(address.thoroughfare_or_desc) > 0:
                    road_names_found.add(address.thoroughfare_or_desc)
                    continue

                found_thoroughfare = False

                for each_line in [address.line_1, address.line_2, address.line_3, address.line_4]:
                    close_matches = difflib.get_close_matches(
                        each_line, roads, cutoff=0.7
                    )
                    
                    if len(close_matches) != 0:
                        match = close_matches[0]
                        address.thoroughfare_or_desc = match
                        road_names_found.add(match)
                        found_thoroughfare = True

                if not found_thoroughfare:
                    not_found_1st.append(address)

            not_found_2nd: Deque[db_repr.SimpleAddress] = deque()

            # Second pass if any road names were found for this postcode
            for address in not_found_1st:
                found_thoroughfare = False
                for each_line in [address.line_1, address.line_2, address.line_3, address.line_4]:
                    for road_name in road_names_found:
                        road_name_l = road_name.lower()

                        if road_name_l in each_line.lower():
                            address.thoroughfare_or_desc = road_name
                            found_thoroughfare = True
                            break

                    if found_thoroughfare:
                        break

                if not found_thoroughfare:
                    not_found_2nd.append(address)

            not_found_3rd: Deque[db_repr.SimpleAddress] = deque()

            # Third pass using slow regex
            for address in not_found_2nd:
                found_thoroughfare = False
                for each_line in [address.line_1, address.line_2, address.line_3, address.line_4]:
                    house_match = re.match(HOUSE_NUMBER_PATTERN, each_line)

                    if house_match is not None:
                        street_group = house_match.group(2)

                        # Exclude po box or ltd
                        match = re.match(LTD_PO_BOX_PATTERN, street_group)

                        if street_group is not None and match is None:
                            address.thoroughfare_or_desc = street_group.strip()
                            found_thoroughfare = True
                            break

                if not found_thoroughfare:
                    not_found_3rd.append(address)

            # Fourth pass, if anything is left over then we just use the last
            # line number that isn't empty
            for address in not_found_3rd:
                lines = [address.line_4, address.line_3, address.line_2, address.line_1]
                
                for line in lines:
                    if len(line) > 0:
                        match = re.match(LTD_PO_BOX_PATTERN, line)

                        if match is None:
                            address.thoroughfare_or_desc = line
                            break

            # Finally, get house names or numbers using regex
            for address in addresses:
                # Attempt to get house number or name
                house_match = re.match(HOUSE_NUMBER_PATTERN, address.line_1)

                if house_match is not None:
                    num_group = house_match.group(1)
                    
                    if num_group is not None:
                        address.house_num_or_name = num_group
                    else:
                        address.house_num_or_name = address.line_1
                else:
                    address.house_num_or_name = address.line_1

            session.commit()

    def get_all_thoroughfares(self):
        try:
            with Session(self.engine) as session:
                distinct_postcodes = session.query(db_repr.OnsPostcode).join(db_repr.SimpleAddress).group_by(db_repr.SimpleAddress.postcode).all()

            self.logger.info(f"Found {len(distinct_postcodes)} distinct postcodes in addresses table")
            process = tqdm.tqdm(
                total=len(distinct_postcodes),
                desc=f"Getting thoroughfares for all postcodes",
            )

            for postcode in distinct_postcodes:
                self.process_thoroughfares_for_postcode(postcode)
                process.update(1)
        except Exception:
            with Session(self.engine) as session:
                update(db_repr.SimpleAddress).values(house_num_or_name='')
                update(db_repr.SimpleAddress).values(thoroughfare_or_desc='')
                session.commit()


if __name__ == "__main__":
    config.init_loggers()
    config.parse_config()

    x = Scraper()
    x.get_all_thoroughfares()
