"""
Imports ONS constituency data and pushes it into the configured database.

See https://geoportal.statistics.gov.uk/. This module parses Westminster Parliamentary Constituencies data from 2022
"""

import enum
import logging
from typing import Dict, Optional

import pandas as pd
from sqlalchemy.orm import Session

from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr

logger = logging.getLogger(__name__)


class ConstituencyField(enum.IntEnum):
    ID = 0
    NAME = 1


class ConstituencyCsvParser:
    """Parses the ONS constituency CSV and writes it to the database"""

    def __init__(self) -> None:
        self.csv = config.conf.input.ons_constituencies_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.engine = db_repr.get_engine()
        self.csv_name = cacher.DatafileName.OnsConstituency

        self.constituency_cache: Dict[str, db_repr.OnsConstituency] = {}
        self.constituency_by_name: Dict[str, db_repr.OnsConstituency] = {}

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(self.csv_name, self.csv)
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing ONS constituencies file")

        rows = pd.read_csv(
            self.csv,
            header=0,
            usecols=[0, 1],
        )

        rows.rename(
            columns={
                rows.columns[0]: db_repr.OnsConstituencyColumnsNames.OID,
                rows.columns[1]: db_repr.OnsConstituencyColumnsNames.NAME,
            },
            inplace=True,
        )
        rows.to_sql(
            db_repr.OnsConstituency.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(
            f"Finished parsing ONS constituencies file, wrote {len(rows.index)} items"
        )

    def get_constituency(
        self, constituency_id: str
    ) -> Optional[db_repr.OnsConstituency]:
        """
        Returns the constituency specified by the ID
        (which is defined by the ONS) if it exists
        """
        session = Session(self.engine)
        try:
            if len(constituency_id) == 0:
                raise ValueError("You must provide a string that isn't empty!")
            elif constituency_id in self.constituency_cache:
                return self.constituency_cache[constituency_id]
            else:
                returned = session.get(db_repr.OnsConstituency, constituency_id)
                if returned is None:
                    return None
                else:
                    self.constituency_cache[constituency_id] = returned
                    return returned
        finally:
            session.close()

    def get_constituency_by_name(
        self, constituency_name: str
    ) -> Optional[db_repr.OnsConstituency]:
        """Returns the constituency by name if it exists. Only performs exact matches."""
        session = Session(self.engine)
        try:
            if len(constituency_name) == 0:
                raise ValueError("You must provide a string that isn't empty!")
            elif constituency_name in self.constituency_by_name:
                return self.constituency_by_name[constituency_name]
            else:
                result = (
                    session.query(db_repr.OnsConstituency)
                    .filter(db_repr.OnsConstituency.name == constituency_name)
                    .one()
                )
                if result is None:
                    return None
                else:
                    self.constituency_by_name[constituency_name] = result
                    return result
        finally:
            session.close()

    def clear_all(self):
        """Deletes all rows in the ONS constituencies table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsConstituency).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)
