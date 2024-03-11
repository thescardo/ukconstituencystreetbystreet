"""
Tools to import use ONS data to get addresses in a postcode that match up to a given constituency.

Uses the getaddress.io API.

The Royal Mail and OS map databases also match up with the data specified above, but because of private
investors the actual address data to match up with the postcodes is not free despite being mostly created
by a (at the time) public entity. See https://en.wikipedia.org/wiki/Postcode_Address_File
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from collections import defaultdict, deque
from dataclasses import dataclass
import multiprocessing
import random
import re
import threading
import time
from typing import Deque, Dict, List, Optional, Set, Tuple
import logging
from sqlalchemy import select, update, func
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
    r"^(\d+[a-zA-Z]{0,1}\s{0,1}[-/]{0,1}\s{0,1}\d*[a-zA-Z]{0,1})\s+(.*)$"
)
LTD_PO_BOX_PATTERN = re.compile(r".*(ltd|po box|plc).*", re.IGNORECASE)

PO_BOX_PATTERN = re.compile(r".*(po box).*", re.IGNORECASE)

PARTIAL_LOOKUP_MAX_NUM_ADDRESSES = 20

MAX_FULL_ADDRESS_LOOKUPS_PER_DAY = 5000

TEMPLATE = (
    r"{line_1}|{line_2}|{line_3}|{line_4}|{town_or_city}|{locality}|{county}|{country}"
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


def get_retry_session(
    retries: int = 20,
    session: Optional[requests.Session] = None,
    backoff_factor: float = 2.0,
):
    """
    Returns a session that will retry many times with backoff. It's not intelligent but it will work if your internet is consistent.
    """
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
    """
    Returns a requests.Response object for a given postcode lookup

    Args:
        postcode: postcode to lookup
        full_lookup: if False, only returns the first 20 results, if True
            returns the full address information (which may be less than 20)
        session: requests.Session required
    """
    if len(postcode) < 5:
        raise ValueError("Postcodes must be 5 characters or longer")

    if session is None:
        session = get_retry_session()

    api_key = config.conf.scraping.get_address_io_api_key

    base_url = f"https://api.getAddress.io/autocomplete/{postcode}?api-key={api_key}"
    # Only do the full lookup when requested
    if full_lookup:
        url = f"{base_url}&all=true&template={TEMPLATE}"
    else:
        url = f"{base_url}&all=false&top={PARTIAL_LOOKUP_MAX_NUM_ADDRESSES}&template={TEMPLATE}"

    headers = {"content-type": "application/json"}

    current: float = 10
    while True:
        response = session.get(url=url, headers=headers)

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
    """Store how much of the daily/monthly limits of getaddress.io we've used"""

    UsageToday: int
    DailyLimit: int
    MonthlyBuffer: int
    MonthlyBufferUsed: int


def get_limit_for_day(session: Optional[requests.Session] = None) -> UsageCounts:
    """Returns the daily/monthly getaddress.io counts we've used"""
    if session is None:
        session = get_retry_session()

    DEFAULT_USAGE = UsageCounts(0, 5000, 500, 0)

    api_key = config.conf.scraping.get_address_io_admin_key.strip()
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
    """
    Acquire a lock with timeout. This doesn't really ensure safe
    running but at least makes the program finish quickly.
    """
    result = lock.acquire(timeout=timeout)
    try:
        yield result
    finally:
        if result:
            lock.release()


class NumAddressReqManager:
    """Manages the usage limits for getaddress.io"""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.Lock()

        self._num_req_remaining = MAX_FULL_ADDRESS_LOOKUPS_PER_DAY
        self._last_date = datetime.now()

        self._usages: UsageCounts = get_limit_for_day()
        self.logger.info(f"Usages limits are: {self._usages}")

    def get_limits(self) -> UsageCounts:
        return get_limit_for_day()

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


class AddrFetcher:
    """Fetches addresses from getaddress.io by constituency."""

    def __init__(self) -> None:
        self.engine = db_repr.get_engine()
        db_repr.Base.metadata.create_all(bind=self.engine)

        self.logger = logging.getLogger(self.__class__.__name__)

        self.num_req_manger = NumAddressReqManager()
        self._db_lock = threading.Lock()

        self.max_simultaneous_loops = 20
        self.use_full_lookups = config.conf.scraping.allow_getting_full_address

        self._dict_lock = threading.Lock()
        self.streets_per_postcode_district: Dict[str, Set[str]] = defaultdict(set)

        # Counts number of API requests this minute
        self._api_counter_lock = threading.Lock()
        self._api_use_last_db_read: Optional[datetime] = None
        self._db_count_last_5_mins: int = 0
        self._api_counter_current_time: Optional[datetime] = None
        self._api_use_counter_this_min: int = 0

    def _get_floored_minute_now(self) -> datetime:
        """
        Returns the current time floored to the minute
        (e.g. 19:56:01.123456 becomes 19:56:00.000000)
        """
        return datetime.now(timezone.utc).replace(second=0, microsecond=0)

    def _get_api_req_count_last_5_minutes(self) -> int:
        """Gets number of API requests made in the last 5 minutes"""
        with Session(self.engine) as db_sess:
            with self._api_counter_lock:
                # Only get time after getting lock
                utc_now = self._get_floored_minute_now()
                self.logger.debug(f"{utc_now=}, {self._api_use_last_db_read=}, {self._db_count_last_5_mins=}, {self._api_use_counter_this_min}")

                # If we have't read from the DB this minute, read the current count now
                if self._api_use_last_db_read != utc_now:
                    num_requests_last_5_minutes = (
                        db_sess.query(func.sum(db_repr.ApiUseLog.num_requests))
                        .where(db_repr.ApiUseLog.minute >= (utc_now - timedelta(minutes=5)))
                        .scalar()
                    )

                    # May not have any rows in the last 5 minutes
                    if num_requests_last_5_minutes is None:
                        self._db_count_last_5_mins = 0
                    else:
                        self._db_count_last_5_mins = num_requests_last_5_minutes

                # Return current db count + not written to db since we're still in the same minute
                return self._db_count_last_5_mins + self._api_use_counter_this_min

    def _can_req_based_on_api_count(self) -> bool:
        """
        Returns whether we can make an API request based off whats
        in the database + counted in RAM.
        
        If the api use count is exceeded, write the current count
        in RAM to the database and return False.
        """
        api_use_in_the_last_5_mins = self._get_api_req_count_last_5_minutes()
        max_requests_with_headroom = config.conf.scraping.max_requests_per_5_mins - 50
        self.logger.debug(
            f"{api_use_in_the_last_5_mins=}, {max_requests_with_headroom=}"
        )
        if (
            api_use_in_the_last_5_mins
            >= max_requests_with_headroom
        ):
            # Update the counter in the database since we've run out of requests
            self._update_api_counter_in_db()
            return False
        else:
            return True

    def _update_api_counter_in_db(self) -> None:
        """Updates api use counter in the database with current counter and resets the counter"""
        if self._api_counter_current_time is None:
            return

        with self._db_lock:
            utc_now = self._get_floored_minute_now()
            with Session(self.engine) as db_sess:
                latest = (
                    db_sess.query(db_repr.ApiUseLog)
                    .order_by(db_repr.ApiUseLog.minute.desc())
                    .first()
                )

                # If an entry already exists then add counter
                if latest is not None and utc_now == latest.minute:
                    latest.num_requests += self._api_use_counter_this_min
                else:
                    db_sess.add(
                        db_repr.ApiUseLog(
                            minute=self._api_counter_current_time,
                            num_requests=self._api_use_counter_this_min,
                        )
                    )

                db_sess.commit()

                # Reset counter for this minute
                self._api_use_counter_this_min = 0
                self._api_counter_current_time = None

    def _add_api_req_count_this_minute(self) -> None:
        """
        Increment local counter for num of API requests, writing the number
        of requests to the database if we go over the minute.
        """
        with self._api_counter_lock:
            # Only get time after getting lock
            utc_now = self._get_floored_minute_now()

            if self._api_counter_current_time != utc_now and self._api_counter_current_time is not None:
                self._update_api_counter_in_db()
            elif self._api_counter_current_time is None:
                self._api_counter_current_time = utc_now

            self._api_use_counter_this_min += 1

    def _get_address_resp_for_postcode_wrapper(self, *args, **kwargs):
        """Wraps get_address_resp_for_postcode by counting API use"""
        # Last case counter in case we wait more than 5 minutes
        counter = 5
        req_count_low_enough = False
        while counter > 0:
            if self._can_req_based_on_api_count():
                self.logger.debug("Not exceeded API limit, making request")
                req_count_low_enough = True
                break
            else:
                wait_time_s = 60
                self.logger.debug(
                    f"Exceeding API limit, waiting {wait_time_s}s"
                )
                time.sleep(wait_time_s)
                continue

        if not req_count_low_enough:
            utc_now = self._get_floored_minute_now()
            raise RuntimeError(
                f"{utc_now=} Waited 5 minutes but API request "
                "count never went down! Programming error!"
            )

        self._add_api_req_count_this_minute()
        return get_address_resp_for_postcode(*args, **kwargs)

    def get_addresses_for_postcode(
        self,
        ons_postcode: db_repr.OnsPostcode,
        http_sess: Optional[requests.Session] = None,
    ) -> Tuple[bool, bool]:
        """Gets addresses for the given postcode"""
        if http_sess is None:
            http_sess = get_retry_session(backoff_factor=10)

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

        resp, parsed = self._get_address_resp_for_postcode_wrapper(
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

        # Check if we've got exactly 20 addresses in the returned data.
        # If so, the given postcode has *AT LEAST* 20 addresses, so we
        # should do a full lookup.
        if num_addresses == PARTIAL_LOOKUP_MAX_NUM_ADDRESSES:
            if not self.use_full_lookups:
                # Only should be reached in a special debug mode
                self.logger.debug(
                    f"{self.use_full_lookups=} Not getting "
                    f"all addresses for {postcode=}"
                )
                return False, False

            if self.num_req_manger.request_and_decrement():
                self.logger.debug(f"Need to get all addresses for {postcode=}")
                resp, parsed = self._get_address_resp_for_postcode_wrapper(
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

        # Now simply parse into a local representation to save to the database
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

        # Write to the database and commit
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

    def fetch_for_local_authority(self, name: str):
        """
        Fetch all addresses for the given local authority by downloading
        all addresses in a given postcode that are in the given constituency
        """
        with Session(self.engine) as session:
            results = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.OnsLocalAuthorityDistrict.name == name)
                .all()
            )
            if len(results) == 0:
                raise ReferenceError(
                    f"Need to have parsed ONS files to scrape data for {name}"
                )

            process = tqdm.tqdm(
                total=len(results),
                desc=f"Fetching addresses for postcodes in {name}",
            )

            for postcode in results:
                self.get_addresses_for_postcode(postcode)
                process.update(1)

    def fetch_for_constituency(self, name: str):
        """
        Fetch all addresses for the given consistency by downloading
        all addresses in a given postcode that are in the given constituency
        """
        with Session(self.engine) as session:
            results = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == name)
                .all()
            )
            if len(results) == 0:
                raise ReferenceError("Need to have parsed ONS files to scrape data")

            process = tqdm.tqdm(
                total=len(results),
                desc=f"Fetching addresses for postcodes in {name}",
            )

            for postcode in results:
                self.get_addresses_for_postcode(postcode)
                process.update(1)

    def fetch_constituencies(self, to_scrape: List[str]):
        """Fetch all addresses for each consituency specified"""
        self.logger.info(f"Scraping addresses for {to_scrape}")

        with Session(self.engine) as session:
            start_num_addresses = session.query(db_repr.SimpleAddress).count()

        for constituency_name in to_scrape:
            self.fetch_for_constituency(constituency_name)

        with Session(self.engine) as session:
            end_num_addresses = session.query(db_repr.SimpleAddress).count()

        new_addresses = end_num_addresses - start_num_addresses
        self.logger.info(f"Scraped {new_addresses} new addresses")

    def fetch_local_authorities(self, to_scrape: List[str]):
        """Fetch all addresses for each local authority specified"""
        self.logger.info(f"Scraping addresses for {to_scrape}")

        with Session(self.engine) as session:
            start_num_addresses = session.query(db_repr.SimpleAddress).count()

        for constituency_name in to_scrape:
            self.fetch_for_local_authority(constituency_name)

        with Session(self.engine) as session:
            end_num_addresses = session.query(db_repr.SimpleAddress).count()

        new_addresses = end_num_addresses - start_num_addresses
        self.logger.info(f"Scraped {new_addresses} new addresses")

    def cleanup_addresses_for_postcode(self, ons_postcode: db_repr.OnsPostcode):
        """
        Performs parsing and clean up of 'thoroughfares' attribute of all addresses
        in a given postcode so that we can guess the house name or number, as well as
        removing PO boxes and the like. If a street name isn't found then don't mess
        with the address.

        This is a pretty inefficient algorithm but since it is only used once per
        constituency we can live with it for the sake of having a relatively simple
        to understand method for clean up of address data.
        """
        with Session(self.engine) as session:
            session.add(ons_postcode)

            addresses = ons_postcode.addresses

            # Fetch all roads that are in the given Postcode from the database. This
            # is done lazily so that we only fetch roads in a given postcode when we
            # need them.
            with self._dict_lock:
                if (
                    ons_postcode.postcode_district
                    not in self.streets_per_postcode_district
                ):
                    os_roads = (
                        session.query(db_repr.OsOpennameRoad)
                        .where(
                            db_repr.OsOpennameRoad.postcode_district
                            == ons_postcode.postcode_district
                        )
                        .all()
                    )

                    roads = self.streets_per_postcode_district[
                        ons_postcode.postcode_district
                    ]
                    for os_road in os_roads:
                        roads.add(os_road.name)
                else:
                    roads = self.streets_per_postcode_district[
                        ons_postcode.postcode_district
                    ]

            not_found_1st: Deque[db_repr.SimpleAddress] = deque()
            road_names_found: Set[str] = set()

            # First pass using difflib
            for address in addresses:
                if len(address.thoroughfare_or_desc) > 0:
                    road_names_found.add(address.thoroughfare_or_desc)
                    continue

                found_thoroughfare = False

                for each_line in [
                    address.line_1,
                    address.line_2,
                    address.line_3,
                    address.line_4,
                ]:
                    # First remove PO boxes, completely useless to us.
                    po_box_match = re.match(PO_BOX_PATTERN, each_line)
                    if po_box_match is not None:
                        # Mark it as found, its a po box so we don't care
                        found_thoroughfare = True
                        break

                    # If the road name matches any of
                    close_matches = difflib.get_close_matches(
                        each_line, roads, cutoff=0.9
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
                for each_line in [
                    address.line_1,
                    address.line_2,
                    address.line_3,
                    address.line_4,
                ]:
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
                for each_line in [
                    address.line_1,
                    address.line_2,
                    address.line_3,
                    address.line_4,
                ]:
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
            # line number that isn't empty as the thoroughfare
            for address in not_found_3rd:
                lines = [address.line_4, address.line_3, address.line_2, address.line_1]

                for line in lines:
                    if len(line) > 0:
                        match = re.match(LTD_PO_BOX_PATTERN, line)

                        if match is None:
                            address.thoroughfare_or_desc = line
                            break

            # Finally, get house names or numbers using regex. If this fails just set
            # the house number or name field to address line 1.
            for address in addresses:
                if address.thoroughfare_or_desc.lower() not in address.line_1.lower():
                    address.house_num_or_name = address.line_1
                else:
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

            with self._db_lock:
                session.commit()

    def cleanup_all_addresses(self):
        """Attempt to cleanup all addresses in each postcode"""
        try:
            with Session(self.engine) as session:
                distinct_postcodes = (
                    session.query(db_repr.OnsPostcode)
                    .join(db_repr.SimpleAddress)
                    .group_by(db_repr.SimpleAddress.postcode)
                    .all()
                )

            self.logger.info(
                f"Found {len(distinct_postcodes)} distinct postcodes in addresses table"
            )
            process = tqdm.tqdm(
                total=len(distinct_postcodes),
                desc="Getting thoroughfares for all postcodes",
            )

            for postcode in distinct_postcodes:
                self.cleanup_addresses_for_postcode(postcode)
                process.update(1)
        except Exception:
            with Session(self.engine) as session:
                update(db_repr.SimpleAddress).values(house_num_or_name="")
                update(db_repr.SimpleAddress).values(thoroughfare_or_desc="")
                session.commit()


if __name__ == "__main__":
    config.init_loggers()
    config.parse_config()

    x = AddrFetcher()
    x.cleanup_all_addresses()
