"""
Middle Layers Super Output Areas - small areas 'comprising between 2,000 and 6,000 households and have a usually resident population between 5,000 and 15,000 persons', see https://www.ons.gov.uk/methodology/geography/ukgeographies/censusgeographies/census2021geographies.

Uses data from 2021

https://geoportal.statistics.gov.uk/datasets/ons::local-authority-districts-december-2023-boundaries-uk-bfe-2/about

https://www.ons.gov.uk/census/maps/choropleth/population/age/resident-age-8c/aged-15-to-24-years

Filtered by msoa

MSOA = "msoa21"
"""


import enum
import logging
from typing import Optional

import pandas as pd
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class OnsMsoaField(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    ENTRY_ID = "FID"
    ID = "MSOA21CD"
    NAME = "MSOA21NM"
    BNG_E = "BNG_E"
    BNG_N = "BNG_N"
    LONGITUDE = "LONG"
    LATITUDE = "LAT"
    SHAPE_AREA = "Shape__Area"
    SHAPE_LENGTH = "Shape__Length"
    GLOBAL_ID = "GlobalID"


class OnsMsoaCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.conf.input.ons_msoa_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.CsvName.OnsMsoa

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(self.csv_name, self.csv)
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing ONS MSOA file")

        rows = pd.read_csv(
            self.csv,
            header=0,
            usecols=[
                OnsMsoaField.ID,
                OnsMsoaField.NAME,
            ],
        )

        rows.rename(
            columns={
                OnsMsoaField.ID: db_repr.OnsMsoaColumnsNames.OID,
                OnsMsoaField.NAME: db_repr.OnsMsoaColumnsNames.NAME,
            },
            inplace=True,
        )

        # print(rows[rows.duplicated(subset=[db_repr.OnsMsoaColumnsNames.ID], keep=False)])

        rows.to_sql(
            db_repr.OnsMsoa.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(
            f"Finished parsing ONS MSOA file, wrote {len(rows.index)} items"
        )

    def get_msoa_by_id(self, msoa_id: str) -> Optional[db_repr.OnsMsoa]:
        with Session(self.engine) as session:
            if len(msoa_id) == 0:
                raise ValueError("You must provide a string that isn't empty!")
            else:
                result = (
                    session.query(db_repr.OnsMsoa)
                    .filter(db_repr.OnsMsoa.oid == msoa_id)
                    .one()
                )
                return result

    def clear_all(self):
        """Clears all rows from the ONS MSOA table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsMsoa).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)


class CensusAgeByMsoaFields(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    MSOA_ID = "Middle layer Super Output Areas Code"
    MSOA_NAME = "Middle layer Super Output Areas"
    AGE_CATEGORY = "Age (101 categories) Code"
    AGE_CATEGORY_NAME = "Age (101 categories)"
    OBSERVED_COUNT = "Observation"


class CensusAgeByMsoaCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.conf.input.census_age_by_msoa_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.CsvName.CensusAgeByMsoa

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
                CensusAgeByMsoaFields.AGE_CATEGORY: int,
                CensusAgeByMsoaFields.OBSERVED_COUNT: int,
            },
            header=0,
            usecols=[
                CensusAgeByMsoaFields.MSOA_ID,
                CensusAgeByMsoaFields.AGE_CATEGORY,
                CensusAgeByMsoaFields.OBSERVED_COUNT,
            ],
        )

        rows.rename(
            columns={
                CensusAgeByMsoaFields.MSOA_ID: "msoa_id",
                CensusAgeByMsoaFields.AGE_CATEGORY: "age_cat",
                CensusAgeByMsoaFields.OBSERVED_COUNT: "observed_count",
            },
            inplace=True,
        )

        # Create new empty columns
        rows["age_range"] = 0
        rows["percent"] = 0
        rows["sum"] = 0

        # Create a percentage of each age 'category'. Each category is 1 year,
        # e.g. 0 or 16, apart from 100 which means 100+
        rows["percent"] = (
            100
            * rows["observed_count"]
            / rows.groupby("msoa_id")["observed_count"].transform("sum")
        )

        print(rows)

        # Assign a category using bins to each row, e.g. 16 will go in the 15-35 bin
        rows["age_range"] = pd.cut(
            rows["age_cat"],
            [0, 16, 35, 100],
            labels=[
                db_repr.CensusAgeRange.R_0_15,
                db_repr.CensusAgeRange.R_16_35,
                db_repr.CensusAgeRange.R_36_100,
            ],
            include_lowest=True,
        )

        # Create a new dataframe that has observed_count and percent summed for all the age ranges
        new_rows = (
            rows.groupby(["msoa_id", "age_range"])[["observed_count", "percent"]]
            .sum()
            .reset_index()
        )

        # Rename to SQL columns
        new_rows.rename(
            columns={
                "msoa_id": db_repr.CensusAgeByMsoaColumnsNames.MSOA_ID,
                "age_range": db_repr.CensusAgeByMsoaColumnsNames.AGE_RANGE,
                "observed_count": db_repr.CensusAgeByMsoaColumnsNames.OBSERVED_COUNT,
                "percent": db_repr.CensusAgeByMsoaColumnsNames.PERCENT_OF_MSOA,
            },
            inplace=True,
        )

        new_rows.to_sql(
            db_repr.CensusAgeByMsoa.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(
            f"Finished parsing Census Age by MSOA file, wrote {len(rows.index)} items"
        )

    def clear_all(self):
        """Clears all rows from the ONS MSOA table"""
        with Session(self.engine) as session:
            session.query(db_repr.CensusAgeByMsoa).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)
