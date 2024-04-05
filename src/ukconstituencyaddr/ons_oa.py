"""
Output Areas - small areas 'comprising between 2,000 and 6,000 households and have a usually resident population between 5,000 and 15,000 persons', see https://www.ons.gov.uk/methodology/geography/ukgeographies/censusgeographies/census2021geographies.

Uses data from 2021

https://www.nomisweb.co.uk/sources/census_2021_bulk

Filtered by oa

Output Area to Lower layer Super Output Area to Middle layer Super Output Area to Local Authority District (December 2021) Lookup in England and Wales v3
For mappings from OA to MSOA and LSOA
"""


import enum
import logging

import pandas as pd
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class OnsOaField(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    OA_ID = "OA21CD"
    LSOA_ID = "LSOA21CD"
    LSOA_NAME = "LSOA21NM"
    LSOA_WARD_NAME = "LSOA21NMW"
    MSOA_ID = "MSOA21CD"
    MSOA_NAME = "MSOA21NM"
    MSOA_WARD_NAME = "MSOA21NMW"
    LAD_ID = "LAD22CD"
    LAD_NAME = "LAD22NM"
    LAD_WARD_NAME = "LAD22NMW"
    OBJECT_ID = "ObjectId"


class OnsOaCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.conf.input.ons_oa_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.DatafileName.OnsOa

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(self.csv_name, self.csv)
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing file")

        rows = pd.read_csv(
            self.csv,
            header=0,
            usecols=[
                OnsOaField.OA_ID,
                OnsOaField.LSOA_ID,
                OnsOaField.MSOA_ID,
                OnsOaField.LAD_ID,
            ],
        )

        rows.rename(
            columns={
                OnsOaField.OA_ID: db_repr.OnsOaColumnsNames.OID,
                OnsOaField.LSOA_ID: db_repr.OnsOaColumnsNames.LSOA_ID,
                OnsOaField.MSOA_ID: db_repr.OnsOaColumnsNames.MSOA_ID,
                OnsOaField.LAD_ID: db_repr.OnsOaColumnsNames.LOCAL_AUTH_DISTRICT_ID,
            },
            inplace=True,
        )

        rows.to_sql(
            db_repr.OnsOa.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(f"Finished parsing file, wrote {len(rows.index)} items")

    def clear_all(self):
        """Clears all rows from the ONS MSOA table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsMsoa).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)


class CensusAgeByOaFields(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    DATE = "date"
    GEOGRAPHY = "geography"
    GEOGRAPHY_CODE = "geography code"
    AGE_TOTAL = "Age: Total"
    AGE_4_AND_UNDER = "Age: Aged 4 years and under"
    AGE_5_TO_9 = "Age: Aged 5 to 9 years"
    AGE_10_TO_14 = "Age: Aged 10 to 14 years"
    AGE_15_TO_19 = "Age: Aged 15 to 19 years"
    AGE_20_TO_24 = "Age: Aged 20 to 24 years"
    AGE_25_TO_29 = "Age: Aged 25 to 29 years"
    AGE_30_TO_34 = "Age: Aged 30 to 34 years"
    AGE_35_to_39 = "Age: Aged 35 to 39 years"
    AGE_40_TO_44 = "Age: Aged 40 to 44 years"
    AGE_45_TO_49 = "Age: Aged 45 to 49 years"
    AGE_50_TO_54 = "Age: Aged 50 to 54 years"
    AGE_55_TO_59 = "Age: Aged 55 to 59 years"
    AGE_60_TO_64 = "Age: Aged 60 to 64 years"
    AGE_65_TO_69 = "Age: Aged 65 to 69 years"
    AGE_70_TO_74 = "Age: Aged 70 to 74 years"
    AGE_75_TO_79 = "Age: Aged 75 to 79 years"
    AGE_80_TO_84 = "Age: Aged 80 to 84 years"
    AGE_85_AND_OVER = "Age: Aged 85 years and over"


class CensusAgeByOaCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.conf.input.census_age_by_oa_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.DatafileName.CensusAgeByMsoa

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_and_set_file_modified(
            self.csv_name, self.csv
        )
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing Census Age by MSOA file")

        rows = pd.read_csv(
            self.csv,
            dtype={
                CensusAgeByOaFields.AGE_TOTAL: int,
                CensusAgeByOaFields.AGE_15_TO_19: int,
                CensusAgeByOaFields.AGE_20_TO_24: int,
                CensusAgeByOaFields.AGE_25_TO_29: int,
                CensusAgeByOaFields.AGE_30_TO_34: int,
            },
            header=0,
            usecols=[
                CensusAgeByOaFields.GEOGRAPHY_CODE,
                CensusAgeByOaFields.AGE_TOTAL,
                CensusAgeByOaFields.AGE_15_TO_19,
                CensusAgeByOaFields.AGE_20_TO_24,
                CensusAgeByOaFields.AGE_25_TO_29,
                CensusAgeByOaFields.AGE_30_TO_34,
            ],
        )

        # Create a percentage of each age 'category'. Each category is 1 year,
        # e.g. 0 or 16, apart from 100 which means 100+
        rows["15_to_34_total"] = rows[
            [
                CensusAgeByOaFields.AGE_15_TO_19,
                CensusAgeByOaFields.AGE_20_TO_24,
                CensusAgeByOaFields.AGE_25_TO_29,
                CensusAgeByOaFields.AGE_30_TO_34,
            ]
        ].sum(axis=1)
        rows["15_to_34_percent"] = 100 * (
            rows["15_to_34_total"] / rows[CensusAgeByOaFields.AGE_TOTAL]
        )

        # Keep only the columns we need
        rows = rows[
            [
                CensusAgeByOaFields.GEOGRAPHY_CODE,
                CensusAgeByOaFields.AGE_TOTAL,
                "15_to_34_total",
                "15_to_34_percent",
            ]
        ].copy()

        # Rename to SQL columns
        rows.rename(
            columns={
                CensusAgeByOaFields.GEOGRAPHY_CODE: db_repr.CensusAgeByOaColumnsNames.OA_ID,
                CensusAgeByOaFields.AGE_TOTAL: db_repr.CensusAgeByOaColumnsNames.AGE_TOTAL,
                "15_to_34_total": db_repr.CensusAgeByOaColumnsNames.TOTAL_15_TO_34,
                "15_to_34_percent": db_repr.CensusAgeByOaColumnsNames.PERCENTAGE_15_TO_34,
            },
            inplace=True,
        )

        rows.to_sql(
            db_repr.CensusAgeByOa.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(f"Finished parsing file, wrote {len(rows.index)} items")

    def clear_all(self):
        """Clears all rows from the ONS OA table"""
        with Session(self.engine) as session:
            session.query(db_repr.CensusAgeByOa).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)
