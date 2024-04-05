from collections import defaultdict, deque
import logging
import multiprocessing
from multiprocessing.pool import AsyncResult
import re
import threading
from typing import Deque, Dict, List, Set
import difflib

from sqlalchemy import update
from sqlalchemy.orm import Session
import tqdm

from ukconstituencyaddr.db import db_repr_sqlite as db_repr
from ukconstituencyaddr.multiprocess_address_cleanup import (
    HOUSE_NUMBER_PATTERN,
    LTD_PO_BOX_PATTERN,
    PO_BOX_PATTERN,
    cleanup_addresses_for_postcode_district,
)
from ukconstituencyaddr.multiprocess_init import multiprocess_init


class AddrFetcher:
    """Fetches addresses from getaddress.io by constituency."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.engine = db_repr.get_engine()

        self._dict_lock = threading.Lock()
        self.streets_per_postcode_outcode: Dict[str, Set[str]] = defaultdict(set)

    def cleanup_addresses_for_postcode(
        self, ons_postcode: db_repr.OnsPostcode
    ) -> List[db_repr.SimpleAddress]:
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
                    not in self.streets_per_postcode_outcode
                ):
                    os_roads = (
                        session.query(db_repr.OsOpennameRoad)
                        .where(
                            db_repr.OsOpennameRoad.postcode_district
                            == ons_postcode.postcode_district
                        )
                        .all()
                    )

                    roads = self.streets_per_postcode_outcode[
                        ons_postcode.postcode_district
                    ]
                    for os_road in os_roads:
                        roads.add(os_road.name)
                else:
                    roads = self.streets_per_postcode_outcode[
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

            with db_repr.DB_THREADING_LOCK:
                session.commit()

    def cleanup_all_addresses(self):
        """Attempt to cleanup all addresses in each postcode"""
        try:
            with Session(self.engine) as session:
                distinct_postcode_districts = session.query(
                    db_repr.OnsPostcode.postcode_district.distinct()
                ).all()

            self.logger.info(
                f"Found {len(distinct_postcode_districts)} distinct postcode districts in addresses table"
            )

            counter = tqdm.tqdm(
                total=len(distinct_postcode_districts),
                desc="Getting thoroughfares for all addresses",
            )

            l = multiprocessing.Lock()
            e = db_repr.get_engine()
            self.logger.debug("created lock")

            with multiprocessing.Pool(
                multiprocessing.cpu_count(),
                initializer=multiprocess_init,
                initargs=(l, e),
            ) as pool:
                self.logger.debug("Started pool")
                results: List[AsyncResult] = []
                for postcode_district in distinct_postcode_districts:
                    results.append(
                        pool.apply_async(
                            cleanup_addresses_for_postcode_district,
                            args=postcode_district,
                        )
                    )

                for x in results:
                    self.logger.debug(f"Finished processing district {x.get()}")
                    counter.update(1)

            self.logger.debug("Finished pool")

            # tqdm(, total=len(distinct_postcode_districts), desc="Getting thoroughfares for all postcodes")
        except Exception:
            with Session(self.engine) as session:
                update(db_repr.SimpleAddress).values(house_num_or_name="")
                update(db_repr.SimpleAddress).values(thoroughfare_or_desc="")
                session.commit()
