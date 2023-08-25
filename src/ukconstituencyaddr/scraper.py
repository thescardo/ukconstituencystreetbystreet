from contextlib import contextmanager
from datetime import datetime
import json
from collections import deque
from dataclasses import dataclass
from http import HTTPStatus
import re
import threading
import time
from typing import Deque, Dict, List, Optional, Tuple
import logging
from urllib3.util.retry import Retry

import requests
from requests.status_codes import codes
import requests.adapters
from sqlalchemy.orm import Session

from ukconstituencyaddr.db import db_repr_sqlite as db_repr
from ukconstituencyaddr import config


PARTIAL_LOOKUP_MAX_NUM_ADDRESSES = 20

MAX_FULL_ADDRESS_LOOKUPS_PER_DAY = 5000

TEMPLATE = "{line_1}|{line_2}|{town_or_city}|{locality}|{county}|{country}"

GET_ADDRESS_IO_SUGGESTIONS_KEY = "suggestions"
GET_ADDRESS_IO_LOCATION_KEY = "location"
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
    retries: int = 5,
    session: Optional[requests.Session] = None,
    backoff_factor: float = 0.3,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
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

    response = requests.get(url=url, headers=headers, params=params)
    if response.status_code == 200:
        return response, json.loads(response.text)
    else:
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
        with acquire_lock_timeout(self._lock) as locked:
            if locked:
                # Check if we need to reset the number of lookups
                time_now = datetime.now()
                if self._last_date.date() != time_now.date():
                    self._usages = get_limit_for_day()

                if self._usages.UsageToday > 0:
                    self._usages.UsageToday -= 1
                    return True
                elif self._usages.MonthlyBufferUsed > 0:
                    self._usages.MonthlyBufferUsed -= 1
                    return True
                else:
                    return False
            else:
                self.logger.info("Failed to acquire lock within timeout!")
                raise TimeoutError("Failed to acquire lock within timeout")


class Scraper:
    def __init__(self) -> None:
        self.engine = db_repr.get_engine()
        db_repr.Base.metadata.create_all(bind=self.engine)

        self.logger = logging.getLogger(self.__class__.__name__)

        self.num_req_manger = NumAddressReqManager()

        self.max_simultaneous_loops = 20

    def get_addresses_for_postcode(
        self, http_sess: requests.Session, postcode: str
    ) -> Tuple[int, bool]:
        db_sess = Session(self.engine)

        # fetched = db_sess.query(db_repr.PostcodeFetched).where(db_repr.PostcodeFetched.postcode == postcode).one_or_none()
        # if fetched is not None and fetched.was_fetched:
        #     return 200, True

        resp, parsed = get_address_resp_for_postcode(
            postcode=postcode, full_lookup=False, session=http_sess
        )

        match resp.status_code:
            case 200:
                pass
            case 429, 503:
                time.sleep(5)
                return resp.reason, False
            case _:
                self.logger.error(f"Unhandled reason code {resp.reason}")
                return resp.reason, False

        # if (
        #     len(parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY])
        #     == FULL_ADDR_LIST_LOOKUP_DATA_FIELD
        # ):
        #     resp, parsed = get_address_resp_for_postcode(
        #         http_sess, postcode=postcode, full_lookup=True
        #     )

        #     match resp.reason:
        #         case HTTPStatus.OK:
        #             pass
        #         case HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.SERVICE_UNAVAILABLE:
        #             time.sleep(5)
        #             return resp.reason, False
        #         case _:
        #             self.logger.error(f"Unhandled reason code {resp.reason}")
        #             return resp.reason, False

        self.logger.info(f"{parsed=}")
        if (
            len(parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY])
            == PARTIAL_LOOKUP_MAX_NUM_ADDRESSES
        ):
            return 200, False

        for item in parsed[GET_ADDRESS_IO_SUGGESTIONS_KEY]:
            if GET_ADDRESS_IO_ID_KEY in item and GET_ADDRESS_IO_LOCATION_KEY in item:
                get_address_io_id = item[GET_ADDRESS_IO_ID_KEY]
                line_list = item[GET_ADDRESS_IO_LOCATION_KEY].split("|")
                line_1 = line_list[0]

                num_match = re.search(r"^(\d+)\s*(.*)$", line_1)
                if num_match is None:
                    house_num_or_name = line_1
                    thoroughfare_or_desc = ""
                else:
                    house_num_or_name = int(num_match.group(1))
                    thoroughfare_or_desc = num_match.group(2)

                db_sess.add(
                    db_repr.SimpleAddress(
                        postcode=postcode,
                        line_1=line_1,
                        line_2=line_list[1],
                        line_3=line_list[2],
                        line_4=line_list[3],
                        house_num_or_name=house_num_or_name,
                        thoroughfare=thoroughfare_or_desc,
                        town_or_city=line_list[4],
                        locality=line_list[5],
                        county=line_list[6],
                        country=line_list[7],
                        get_address_io_id=get_address_io_id,
                    )
                )

        db_sess.add(db_repr.PostcodeFetched(postcode=postcode, was_fetched=True))
        db_sess.commit()
        db_sess.close()
        return resp.status_code, True

    def scrape(self, constituencies_to_scrape: List[str]):
        self.logger.info(f"Scraping addresses for {constituencies_to_scrape}")
        postcodes_to_search: Deque[str] = deque()

        with Session(self.engine) as session:
            for constituency_name in constituencies_to_scrape:
                results = (
                    session.query(db_repr.OnsPostcode.postcode)
                    .join(db_repr.OnsConstituency)
                    .where(db_repr.OnsConstituency.name == constituency_name)
                    .all()
                )
                for result in results:
                    postcodes_to_search.append(result.tuple()[0])

        self.logger.info(f"Make list of len {len(postcodes_to_search)} to scrape")


if __name__ == "__main__":
    config.parse_config()
    config.init_loggers()
    x = Scraper()
    x.get_addresses_for_postcode(retry_session(), "YO318JH")
