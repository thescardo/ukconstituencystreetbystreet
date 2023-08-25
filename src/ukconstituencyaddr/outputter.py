""""""
import argparse
import concurrent.futures
import logging
import multiprocessing
from typing import Optional

import pandas as pd
import tqdm
from sqlalchemy.orm import Session

from ukconstituencyaddr import (
    config,
    ons_constituencies,
    ons_postcodes,
    scraper,
)
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class ConstituencyInfoOutputter:
    def __init__(self) -> None:
        self.constituency_parser = ons_constituencies.ConstituencyCsvParser()
        self.postcode_parser = ons_postcodes.PostcodeCsvParser(self.constituency_parser)
        # self.paf_parser = royal_mail_paf.PafCsvParser()
        self.paf_scraper = scraper.Scraper()

        self.output_folder = config.config.output.output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.use_subfolders = config.config.output.use_subfolders

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

    def get_constituency_folder(self, constituency_name: str):
        if self.use_subfolders:
            constituency_output = self.output_folder / constituency_name
            constituency_output.mkdir(parents=True, exist_ok=True)
        else:
            constituency_output = self.output_folder

        return constituency_output

    def process_csvs(self):
        parsers = [self.constituency_parser, self.postcode_parser]
        process = tqdm.tqdm(total=len(parsers), desc="Importing CSVs to local database")
        for x in parsers:
            try:
                x.process_csv()
            except:
                self.logger.error("Caught exception, clearing constituency cache")
                x.clear_all()
                raise
            process.update(1)

    def scrape(self):
        self.paf_scraper.parse_addresses_for_postcode("S25PX", scraper.TEST_DATA)

    def make_csv_streets_in_constituency(
        self,
        constituency_name: Optional[str] = None,
        constituency_id: Optional[str] = None,
    ):
        assert constituency_id is not None or constituency_name is not None
        session = Session(self.engine)

        try:
            if constituency_name is None:
                constituency_name = self.constituency_parser.get_constituency(
                    constituency_id
                ).name

            base_query = (
                session.query(db_repr.RoyalMailPaf)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
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
                db_repr.RoyalMailPaf.thoroughfare_and_desc
            ).with_entities(db_repr.RoyalMailPaf.thoroughfare_and_desc)

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(
                    f"Found no addresses for constituency {constituency_name}"
                )
            else:
                dir = self.get_constituency_folder(constituency_name)
                df.to_csv(str(dir / f"{constituency_name} Street Names.csv"))
        finally:
            session.close()

    def make_csv_addresses_in_constituency(
        self,
        constituency_name: Optional[str] = None,
        constituency_id: Optional[str] = None,
    ):
        assert constituency_id is not None or constituency_name is not None
        session = Session(self.engine)

        try:
            if constituency_name is None:
                constituency_name = self.constituency_parser.get_constituency(
                    constituency_id
                ).name

            base_query = (
                session.query(db_repr.RoyalMailPaf)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
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
        finally:
            session.close()

    def make_csvs_for_all_constituencies(self):
        session = Session(self.engine)
        try:
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
        finally:
            session.close()


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
        help="If specified, scrapes data from royal mail",
    )

    args = parser.parse_args()

    if not args.init_config:
        init_loggers()
        config.parse_config()

        comb = ConstituencyInfoOutputter()

        if not args.build_cache:
            if args.scrape:
                comb.scrape()
                return

            comb.process_csvs()
            if args.constituency is not None:
                comb.make_csv_streets_in_constituency(args.constituency)
            else:
                comb.make_csvs_for_all_constituencies()
