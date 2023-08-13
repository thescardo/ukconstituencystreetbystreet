""""""
import argparse
from collections import deque
import logging
from typing import Deque, List

from sqlalchemy import select
from sqlalchemy.orm import Session
import pandas as pd

import ons_constituencies
import ons_postcodes
import royal_mail_paf
from db import db_repr_sqlite as db_repr


def init_loggers():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("constituencystreetchk.log"),
        ],
    )
    logging.getLogger()

    for name in ["sqlalchemy", "sqlalchemy.engine"]:
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)


class Combinator:
    def __init__(self) -> None:
        self.constituency_parser = ons_constituencies.ConstituencyCsvParser()
        self.postcode_parser = ons_postcodes.PostcodeCsvParser(self.constituency_parser)
        self.paf_parser = royal_mail_paf.PafCsvParser()

        self.output_file = "test.csv"

        self.cache_db_file, self.engine = db_repr.get_engine()
        self.session: Session

        self.logger = logging.getLogger(self.__class__.__name__)

    def process_csvs(self):
        try:
            self.constituency_parser.process_csv()
        except:
            self.logger.error("Caught exception, clearing constituency cache")
            self.constituency_parser.clear_all()
            raise

        try:
            self.postcode_parser.process_csv()
        except:
            self.logger.error("Caught exception, clearing postcode cache")
            self.postcode_parser.clear_all()
            raise

        try:
            self.paf_parser.process_csv()
        except:
            self.logger.error("Caught exception, clearing paf cache")
            self.paf_parser.clear_all()
            raise

    @db_repr.wrap_session
    def make_csv_of_contituency(self, constituency_name: str):
        # constituency = self.constituency_parser.get_constituency_by_name(
        #     constituency_name
        # )
        # if constituency is None:
        #     raise Exception(f"Unable to find constituency {constituency_name}")
        # else:
        #     self.logger.debug(f"Got constituency {constituency}!")

        # self.session.add(constituency)
        selectable = (
            self.session.query(db_repr.RoyalMailPaf)
            .join(db_repr.OnsPostcode)
            .join(db_repr.OnsConstituency)
            .filter(db_repr.OnsConstituency.name == constituency_name)
            .distinct(db_repr.RoyalMailPaf.thoroughfare_and_desc)
            .with_entities(db_repr.RoyalMailPaf.thoroughfare_and_desc)
        ).selectable

        df = pd.read_sql(selectable, self.engine)
        df.to_csv(f"{constituency_name} Street Names.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="UKConstituencyStreetCheck",
        description="Processes ONS data along with Royal Mail data "
        "to produce lists of addresses in a constituency",
        epilog="You need to download the data yourself, see the README",
    )

    parser.add_argument("-g", "--graphicmode", action="store_true")  # on/off flag

    init_loggers()

    comb = Combinator()
    comb.process_csvs()
    comb.make_csv_of_contituency("York Central")
