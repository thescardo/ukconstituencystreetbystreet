import enum
import logging
import pathlib
from typing import List, Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from ukconstituencyaddr.config import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr

logger = logging.getLogger(__name__)


class RoyalMailPafField(enum.IntEnum):
    POSTCODE = 0
    POST_TOWN = 1
    DEPENDENT_LOCALITY = 2
    DOUBLE_DEPENDENT_LOCALITY = 3
    THOROUGHFARE_AND_DESC = 4
    DOUBLE_THOROUGHFARE_AND_DESC = 5
    BUILDING_NUM = 6
    BUILDING_NAME = 7
    SUB_BUILDING_NAME = 8
    PO_BOX = 9
    DEPARTMENT_NAME = 10
    ORG_NAME = 11
    UDPRN = 12
    POSTCODE_TYPE = 13
    SU_ORG_IND = 14
    DELIVERY_POINT_SUFFIX = 15
    ADDR_KEY = 16
    ORG_KEY = 17
    NUM_HOUSEHOLDS = 18
    LOCALITY_KEY = 19


class PafCsvParser:
    def __init__(self) -> None:
        self.csv = config.input.royal_mail_paf_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.CsvName.RoyalMailPaf

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Using CSV {self.csv}")

    def process_row(self, row: List[str]) -> Optional[db_repr.RoyalMailPaf]:
        postcode = row[RoyalMailPafField.POSTCODE].replace(" ", "")
        try:
            return db_repr.RoyalMailPaf(
                postcode=postcode,
                post_town=row[RoyalMailPafField.POST_TOWN],
                dependent_locality=row[RoyalMailPafField.DEPENDENT_LOCALITY],
                double_dependent_locality=row[
                    RoyalMailPafField.DOUBLE_DEPENDENT_LOCALITY
                ],
                thoroughfare_and_desc=row[RoyalMailPafField.THOROUGHFARE_AND_DESC],
                double_thoroughfare_and_desc=row[
                    RoyalMailPafField.DOUBLE_THOROUGHFARE_AND_DESC
                ],
                building_num=row[RoyalMailPafField.BUILDING_NUM],
                building_name=row[RoyalMailPafField.BUILDING_NAME],
                sub_building_name=row[RoyalMailPafField.SUB_BUILDING_NAME],
                po_box=row[RoyalMailPafField.PO_BOX],
                department_name=row[RoyalMailPafField.DEPARTMENT_NAME],
                org_name=row[RoyalMailPafField.ORG_NAME],
                udprn=row[RoyalMailPafField.UDPRN],
                postcode_type=row[RoyalMailPafField.POSTCODE_TYPE],
                su_org_ind=row[RoyalMailPafField.SU_ORG_IND],
                delivery_point_suffix=row[RoyalMailPafField.DELIVERY_POINT_SUFFIX],
                addr_key=int(row[RoyalMailPafField.ADDR_KEY]),
                org_key=int(row[RoyalMailPafField.ORG_KEY]),
                num_households=int(row[RoyalMailPafField.NUM_HOUSEHOLDS]),
                locality_key=int(row[RoyalMailPafField.LOCALITY_KEY]),
            )
        except Exception as e:
            raise Exception(f"Failed to process {row=}") from e

    @db_repr.wrap_session
    def process_csv(self):
        modified = cacher.DbCacheInst.check_and_set_file_modified(
            self.csv_name, self.csv
        )
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info(f"Parsing PAF file")

        def strip_spaces(x: str):
            return x.replace(" ", "")

        rows = pd.read_csv(
            self.csv,
            dtype={},
            header=0,
            names=[
                db_repr.PafColumnNames.POSTCODE,
                db_repr.PafColumnNames.POST_TOWN,
                db_repr.PafColumnNames.DEPENDENT_LOCALITY,
                db_repr.PafColumnNames.DOUBLE_DEPENDENT_LOCALITY,
                db_repr.PafColumnNames.THOROUGHFARE_AND_DESC,
                db_repr.PafColumnNames.DOUBLE_THOROUGHFARE_AND_DESC,
                db_repr.PafColumnNames.BUILDING_NUM,
                db_repr.PafColumnNames.BUILDING_NAME,
                db_repr.PafColumnNames.SUB_BUILDING_NAME,
                db_repr.PafColumnNames.PO_BOX,
                db_repr.PafColumnNames.DEPARTMENT_NAME,
                db_repr.PafColumnNames.ORG_NAME,
                db_repr.PafColumnNames.UDPRN,
                db_repr.PafColumnNames.POSTCODE_TYPE,
                db_repr.PafColumnNames.SU_ORG_IND,
                db_repr.PafColumnNames.DELIVERY_POINT_SUFFIX,
                db_repr.PafColumnNames.ADDR_KEY,
                db_repr.PafColumnNames.ORG_KEY,
                db_repr.PafColumnNames.NUM_HOUSEHOLDS,
                db_repr.PafColumnNames.LOCALITY_KEY,
            ],
            converters={
                db_repr.PafColumnNames.POSTCODE: strip_spaces,
                db_repr.PafColumnNames.THOROUGHFARE_AND_DESC: str.strip,
                db_repr.PafColumnNames.BUILDING_NUM: strip_spaces,
                db_repr.PafColumnNames.UDPRN: strip_spaces,
                db_repr.PafColumnNames.ADDR_KEY: strip_spaces,
                db_repr.PafColumnNames.ORG_KEY: strip_spaces,
                db_repr.PafColumnNames.NUM_HOUSEHOLDS: strip_spaces,
                db_repr.PafColumnNames.LOCALITY_KEY: strip_spaces,
            },
        )

        for col_name in [
            db_repr.PafColumnNames.BUILDING_NUM,
            db_repr.PafColumnNames.UDPRN,
            db_repr.PafColumnNames.ADDR_KEY,
            db_repr.PafColumnNames.ORG_KEY,
            db_repr.PafColumnNames.NUM_HOUSEHOLDS,
            db_repr.PafColumnNames.LOCALITY_KEY,
        ]:
            rows[col_name].replace("", np.nan, inplace=True)
            rows[col_name] = pd.to_numeric(rows[col_name])

        rows.to_sql(
            db_repr.RoyalMailPaf.__tablename__,
            self.engine,
            if_exists="append",
            index=True,
            index_label=db_repr.PafColumnNames.ID,
            chunksize=100000,
        )

        self.logger.info(f"Finished parsing PAF file, wrote {len(rows.index)} items")

    def clear_all(self):
        session = Session(self.engine)
        try:
            session.query(db_repr.RoyalMailPaf).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)
        finally:
            session.close()
