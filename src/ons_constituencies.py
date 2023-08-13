import csv
import enum
import logging
import math
import pathlib
import time
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import select
import tqdm
from sqlalchemy.orm import Session

from db import cacher
from db import db_repr_sqlite as db_repr

logger = logging.getLogger(__name__)

ONS_CONSTITUENCY_CSV = pathlib.Path(
    "/home/the/Workspace/GNDR/postcode_lookup/ONS Data/Westminster_Parliamentary_Constituencies_(December_2022)_Names_and_Codes_in_the_United_Kingdom.csv"
)


class ConstituencyField(enum.IntEnum):
    ID = 0
    NAME = 1


class ConstituencyCsvParser:
    def __init__(self, paf_csv: pathlib.Path = ONS_CONSTITUENCY_CSV) -> None:
        self.csv = paf_csv
        assert self.csv.exists()

        self.cache_db_file, self.engine = db_repr.get_engine()
        self.session = Session(self.engine)
        self.csv_name = cacher.CsvName.OnsConstituency

        self.constituency_cache: Dict[str, db_repr.OnsConstituency] = {}
        self.constituency_by_name: Dict[str, db_repr.OnsConstituency] = {}

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        modified = cacher.DbCacheInst.check_and_set_file_modified(
            self.csv_name, self.csv
        )
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing ONS constituencies file")

        rows = pd.read_csv(
            self.csv,
            header=0,
            usecols=[0, 1],
        )

        rows.rename(columns={"PCON22CD": "id", "PCON22NM": "name"}, inplace=True)
        rows.to_sql(
            db_repr.OnsConstituency.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            index_label="id",
            chunksize=100000,
        )

        self.logger.info(
            f"Finished parsing ONS constituencies file, wrote {len(rows.index)} items"
        )

    @db_repr.wrap_session
    def get_constituency(
        self, constituency_id: str
    ) -> Optional[db_repr.OnsConstituency]:
        if len(constituency_id) == 0:
            raise ValueError("You must provide a string that isn't empty!")
        elif constituency_id in self.constituency_cache:
            return self.constituency_cache[constituency_id]
        else:
            returned = self.session.get(db_repr.OnsConstituency, constituency_id)
            if returned is None:
                return None
            else:
                self.constituency_cache[constituency_id] = returned
                return returned

    @db_repr.wrap_session
    def get_constituency_by_name(
        self, constituency_name: str
    ) -> Optional[db_repr.OnsConstituency]:
        if len(constituency_name) == 0:
            raise ValueError("You must provide a string that isn't empty!")
        elif constituency_name in self.constituency_by_name:
            return self.constituency_by_name[constituency_name]
        else:
            result = (
                self.session.query(db_repr.OnsConstituency)
                .filter(db_repr.OnsConstituency.name == constituency_name)
                .one()
            )
            if result is None:
                return None
            else:
                self.constituency_by_name[constituency_name] = result
                return result

    @db_repr.wrap_session
    def clear_all(self):
        self.session.query(db_repr.OnsConstituency).delete()
        self.session.commit()
        cacher.DbCacheInst.clear_file_modified(self.csv_name)
