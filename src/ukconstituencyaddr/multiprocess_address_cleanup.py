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

from typing import Set

from ukconstituencyaddr.db import db_repr_sqlite as db_repr

HOUSE_NUMBER_PATTERN = re.compile(
    r"^(\d+[a-zA-Z]{0,1}\s{0,1}[-/]{0,1}\s{0,1}\d*[a-zA-Z]{0,1})\s+(.*)$"
)
LTD_PO_BOX_PATTERN = re.compile(r".*(ltd|po box|plc).*", re.IGNORECASE)

PO_BOX_PATTERN = re.compile(r".*(po box).*", re.IGNORECASE)


def multiprocess_init(l, e):
    global db_write_lock, engine
    db_write_lock = l
    engine = e

    engine.dispose(close=False)


def cleanup_addresses_for_postcode_district(postcode_district: str) -> str:
    """
    Performs parsing and clean up of 'thoroughfares' attribute of all addresses
    in a given postcode so that we can guess the house name or number, as well as
    removing PO boxes and the like. If a street name isn't found then don't mess
    with the address.

    This is a pretty inefficient algorithm but since it is only used once per
    constituency we can live with it for the sake of having a relatively simple
    to understand method for clean up of address data.
    """
    global db_write_lock

    roads_in_district: Set[str] = set()

    try:
        with Session(engine) as session:
            addresses = (
                session.query(db_repr.SimpleAddress)
                .where(db_repr.SimpleAddress.postcode == db_repr.OnsPostcode.postcode)
                .where(db_repr.OnsPostcode.postcode_district == postcode_district)
                .all()
            )

            # Fetch all roads that are in the given Postcode from the database.
            os_roads = (
                session.query(db_repr.OsOpennameRoad)
                .where(db_repr.OsOpennameRoad.postcode_district == postcode_district)
                .all()
            )

            for os_road in os_roads:
                roads_in_district.add(os_road.name)

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
                        each_line, roads_in_district, cutoff=0.9
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

            with db_write_lock:
                session.commit()

            return postcode_district
    except:
        print(f"Exception occured!")
        raise  # Re-raise the exception so that the process exits
