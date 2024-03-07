""""""
import argparse
import concurrent.futures
import logging
import multiprocessing
from typing import List, Optional
import difflib
import pathlib

import pandas as pd
import tqdm
from sqlalchemy.orm import Session

from ukconstituencyaddr import (
    address_fetcher,
    config,
    ons_constituencies,
    ons_postcodes,
    os_opennames,
)
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class ConstituencyInfoOutputter:
    """Class that outputs csvs of addresses based on a constituency or other desired attribute"""

    def __init__(self) -> None:
        self.constituency_parser = ons_constituencies.ConstituencyCsvParser()
        self.postcode_parser = ons_postcodes.PostcodeCsvParser()
        self.osopennames_parser = os_opennames.OsOpenNamesCsvsParser()
        self.street_fetcher = address_fetcher.AddrFetcher()

        self.output_folder = config.config.output.output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.use_subfolders = config.config.output.use_subfolders

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

    def get_constituency_folder(self, constituency_name: str) -> pathlib.Path:
        """Returns the Path of the given constituency"""
        if self.use_subfolders:
            constituency_output = self.output_folder / constituency_name
            constituency_output.mkdir(parents=True, exist_ok=True)
        else:
            constituency_output = self.output_folder

        return constituency_output

    def process_csvs(self):
        """
        Parses all CSVs that are required to map constituencies
        and other information onto a given address
        """
        parsers = [self.constituency_parser, self.postcode_parser, self.osopennames_parser]
        process = tqdm.tqdm(total=len(parsers), desc="Importing CSVs to local database")
        for x in parsers:
            try:
                x.process_csv()
            except:
                self.logger.error("Caught exception, clearing constituency cache")
                x.clear_all()
                raise
            process.update(1)

    def fetch_addresses_in_constituency(self, constituency_names: List[str]):
        """Downloads all address data for the given constituency names"""
        self.street_fetcher.fetch(constituency_names)

    def make_csv_streets_in_constituency(
        self,
        constituency_name: Optional[str] = None,
        constituency_id: Optional[str] = None,
    ):
        """Make CSV of all streets in a given constituency"""
        assert constituency_id is not None or constituency_name is not None
        with Session(self.engine) as session:
            if constituency_name is None:
                constituency_name = self.constituency_parser.get_constituency(
                    constituency_id
                ).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency).where(db_repr.SimpleAddress.thoroughfare_or_desc != None).where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if constituency_id is not None:
                mid_query = base_query.filter(
                    db_repr.OnsConstituency.id == constituency_id
                )
            else:
                mid_query = base_query.filter(
                    db_repr.OnsConstituency.name == constituency_name
                )

            final_query = mid_query.distinct(
                db_repr.SimpleAddress.thoroughfare_or_desc
            ).with_entities(db_repr.SimpleAddress.thoroughfare_or_desc)

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(
                    f"Found no addresses for constituency {constituency_name}"
                )
            else:
                dir = self.get_constituency_folder(constituency_name)
                df.to_csv(str(dir / f"{constituency_name} Street Names.csv"))

    def make_csv_addresses_in_constituency(
        self,
        constituency_name: Optional[str] = None,
        constituency_id: Optional[str] = None,
    ):
        """Make CSV of all addresses in a given constituency"""
        assert constituency_id is not None or constituency_name is not None
        with Session(self.engine) as session:
            if constituency_name is None:
                constituency_name = self.constituency_parser.get_constituency(
                    constituency_id
                ).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency).where(db_repr.SimpleAddress.thoroughfare_or_desc != None).where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if constituency_id is not None:
                final_query = base_query.filter(
                    db_repr.OnsConstituency.id == constituency_id
                )
            else:
                final_query = base_query.filter(
                    db_repr.OnsConstituency.name == constituency_name
                )

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(
                    f"Found no addresses for constituency {constituency_name}"
                )
            else:
                dir = self.get_constituency_folder(constituency_name)
                df.to_csv(str(dir / f"{constituency_name} Addresses.csv"))

    def make_csvs_for_all_constituencies(self):
        """
        For all constituencies in the database, make a CSV of
        addresses in each constituency
        """
        with Session(self.engine) as session:
            all_constituencies = [
                constituency.id
                for constituency in session.query(db_repr.OnsConstituency).all()
            ]

            def make_csvs_for_constituency(constituency_id: str) -> bool:
                self.make_csv_streets_in_constituency(constituency_id=constituency_id)
                self.make_csv_addresses_in_constituency(constituency_id=constituency_id)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=multiprocessing.cpu_count()
            ) as executor:
                results = list(
                    tqdm.tqdm(
                        executor.map(make_csvs_for_constituency, all_constituencies),
                        total=len(all_constituencies),
                        desc="Outputting addresses to CSV",
                    )
                )
            return results

    def get_similar_constituencies(self, search_term: str) -> List[str]:
        """Returns constituencies that match the name of the search term"""
        with Session(self.engine) as session:
            constituencies = list(session.query(db_repr.OnsConstituency).all())
            constituency_names = [constituency.name for constituency in constituencies]

            return difflib.get_close_matches(
                search_term, constituency_names, n=5, cutoff=0.3
            )

    def percent_fetched_for_constituency(self, constituency_name: str):
        """
        Prints the percentage of postcode areas that address data
        has been fetched for in a given constituency
        """
        with Session(self.engine) as session:
            num_postcodes_in_constituency = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == constituency_name)
                .count()
            )
            num_postcodes_fetched = (
                session.query(db_repr.PostcodeFetched)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == constituency_name)
                .count()
            )
            percent = (num_postcodes_fetched / num_postcodes_in_constituency) * 100

            print_str = f"In {constituency_name}, {num_postcodes_in_constituency=}, {num_postcodes_fetched=}, {percent=}"
            self.logger.info(
                f"In {constituency_name}, {num_postcodes_in_constituency=}, {num_postcodes_fetched=}, {percent=}"
            )
            print(print_str)


def output_csvs():
    parser = argparse.ArgumentParser(
        prog="UKConstituencyStreetCheck",
        description="Processes ONS data along with Royal Mail data "
        "to produce lists of addresses in a constituency."
        "When run the first time, it will create a folder where it will"
        "keep all of its config and local cache, and another folder"
        "(configurable) to output all CSVs that you wish to create.",
        epilog="You need to download the data yourself, see the README",
    )
    parser.add_argument(
        "-i",
        "--init_config",
        action="store_true",
        help="If specified, only initialise config and config folder",
    )
    parser.add_argument(
        "-c",
        "--constituency",
        help="If specified, only output CSVs for that constituency",
    )
    parser.add_argument(
        "-b",
        "--build_cache",
        action="store_true",
        help="If specified, build the local database cache",
    )
    parser.add_argument(
        "-s",
        "--scrape",
        action="store_true",
        help="If specified, scrapes data from getaddress.io",
    )
    parser.add_argument(
        "-n",
        "--num_scraped",
        action="store_true",
        help="If specified, return percentage of fetched "
        "postcodes from constituency argument",
    )

    args = parser.parse_args()

    if not args.init_config:
        config.init_loggers()
        config.parse_config()

        comb = ConstituencyInfoOutputter()

        if args.num_scraped:
            comb.percent_fetched_for_constituency(args.constituency)
            return

        if args.scrape:
            comb.fetch_addresses_in_constituency([args.constituency])
            return

        if args.build_cache:
            comb.process_csvs()

        if args.constituency:
            comb.make_csv_streets_in_constituency(constituency_name=args.constituency)
            comb.make_csv_addresses_in_constituency(constituency_name=args.constituency)
