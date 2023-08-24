import json
import math
import pathlib
import random
from collections import deque
from dataclasses import dataclass
from http import HTTPStatus
import asyncio
import time
from typing import Deque, Dict, List, Optional
import logging
import concurrent.futures
import ssl
from urllib3 import poolmanager

import requests
import requests.adapters
from sqlalchemy.orm import Session
from sqlalchemy import select

from ukconstituencyaddr.db import db_repr_sqlite as db_repr


GET_ADDRESS_API = ""
MAX_RESULTS_FROM_QUERY = 20

TEMPLATE = "{line_1}|{line_2}|{town_or_city}|{locality}|{county}|{country}"

ONLY_FIRST_20_RESULTS = json.dumps(
    {
        "all": False,
        "top": MAX_RESULTS_FROM_QUERY,
        "template": TEMPLATE,
    }
)

ALL_RESULTS = json.dumps(
    {
        "all": True,
        "template": TEMPLATE,
    }
)


def get_address_resp_for_postcode(
    postcode: str,
) -> Optional[requests.Response]:
    if len(postcode) < 5:
        raise ValueError("Postcodes must be 5 characters or longer")

    url = f"https://api.getAddress.io/autocomplete/{postcode}?api-key={GET_ADDRESS_API}"
    headers = {"content-type": "application/json"}
    data = ONLY_FIRST_20_RESULTS
    response = requests.get(url=url, headers=headers, data=data)
    if response.reason == HTTPStatus.OK:
        return response
    else:
        return None


class Scraper:
    def __init__(self) -> None:
        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.max_simultaneous_loops = 20
        self.min_session_requests = 5
        self.max_session_requests = 10
        self.min_sleep_between_requests = 1
        self.max_sleep_between_requests = 20

    def parse_addresses_for_postcode(self, postcode: str, resp_text: str) -> None:
        session = Session(self.engine)

        parsed = json.loads(resp_text)

        for item in parsed[ITEMS_NAME]:
            try:
                if item[TYPE_FIELD] == ADDRESS_TYPE:
                    session.add(
                        db_repr.SimpleAddress(
                            postcode=postcode, address_text=item[ADDRESS_TEXT_FIELD]
                        )
                    )
            except Exception:
                pass
        session.commit()
        session.close()

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
