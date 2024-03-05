"""
Imports ONS postcode data and pushes it into the configured database.

See https://geoportal.statistics.gov.uk/. This module parses National Statistics Postcode Lookup (NSPL) - 2021 Census (February 2024) data.
"""

import enum
import logging

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class OnsPostcodeField(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""
    POSTCODE = "pcd"
    POSTCODE_2 = "pcd2"
    POSTCODE_VAR = "pcds"
    DATE_OF_INTR = "dointr"
    DATE_OF_TERM = "doterm"
    POSTCODE_USER_TYPE = "usertype"
    NAT_GRID_REF_EAST = "oseast1m"
    NAT_GRID_REF_NORTH = "osnrth1m"
    GRID_REF_POS_QUAL_IDX = "osgrdind"
    OUTPUT_AREA_CENSUS_21 = "oa21"
    COUNTY = "cty"
    COUNTY_ELECTORAL_DIV = "ced"
    LOCAL_AUTHORITY_DISTRICT = "laua"
    ELECTORAL_WARD = "ward"
    FORMER_STRAT_HEALTH_AUTHORITY = "hlthau"
    NHS_ENGLAND_REGION = "nhser"
    COUNTRY = "ctry"
    REGION = "rgn"
    WESTMINISTER_PARLIAMENTRY_CONSTITUENCY = "pcon"
    EUROPEAN_ELECTORAL_REGION = "eer"
    LOCAL_LEARNING_SKILLS_COUNCIL = "teclec"
    TRAVEL_TO_WORK_AREA = "ttwa"
    PRIMARY_CARE_TRUST = "pct"
    INTERNATIONAL_TERRITORIAL_LEVEL = "itl"
    # Described as 'npark' in the dev guide but its
    # actually park in the data
    NATIONAL_PARK = "park"
    LL_SUPER_OUTPUT_AREA_CENSUS_21 = "lsoa21"
    ML_SUPER_OUTPUT_AREA_CENSUS_21 = "msoa21"
    WORKPLACE_ZONE_CENSUS_11 = "wz11"
    SUB_ICB_LOCATION = "ccg"
    BUILT_UP_AREA = "bua11"
    BUILT_UP_AREA_SUB_DIV = "buasd11"
    RURAL_URBAN_CLASS_CENSUS_11 = "ru11ind"
    OUTPUT_AREA_CLASS_CENSUS_11 = "oac11"
    DECIMAL_DEGRESS_LAT = "lat"
    DECIMAL_DEGRESS_LONG = "long"
    LOCAL_ENTERPRISE_PARTNERSHIP_1 = "lep1"
    LOCAL_ENTERPRISE_PARTNERSHIP_2 = "lep2"
    POLICE_FORCE_AREA = "pfa"
    INDEX_OF_MULTIPLE_DEPRIVATION = "imd"
    CANCER_ALLIANCE = "calncv"
    INTEGRATED_CARE_BOARD = "stp"


class PostcodeCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.config.input.ons_postcodes_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.CsvName.OnsPostcode

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

        self.logger.info("Parsing ONS postcodes file")

        def strip_spaces(x: str):
            return x.replace(" ", "")

        rows = pd.read_csv(
            self.csv,
            dtype={
                OnsPostcodeField.POSTCODE_USER_TYPE: int,
                OnsPostcodeField.NAT_GRID_REF_EAST: int,
                OnsPostcodeField.NAT_GRID_REF_NORTH: int,
                OnsPostcodeField.GRID_REF_POS_QUAL_IDX: int,
                OnsPostcodeField.POSTCODE_USER_TYPE: int,
                OnsPostcodeField.DECIMAL_DEGRESS_LAT: float,
                OnsPostcodeField.DECIMAL_DEGRESS_LONG: float,
                OnsPostcodeField.INDEX_OF_MULTIPLE_DEPRIVATION: float,
            },
            header=0,
            converters={
                OnsPostcodeField.POSTCODE: strip_spaces,
            },
            usecols=[
                OnsPostcodeField.POSTCODE,
                OnsPostcodeField.COUNTRY,
                OnsPostcodeField.REGION,
                OnsPostcodeField.WESTMINISTER_PARLIAMENTRY_CONSTITUENCY,
                OnsPostcodeField.ELECTORAL_WARD,
            ],
        )

        rows.rename(
            columns={
                OnsPostcodeField.POSTCODE: db_repr.OnsPostcodeColumnNames.POSTCODE,
                OnsPostcodeField.COUNTRY: db_repr.OnsPostcodeColumnNames.COUNTRY_ID,
                OnsPostcodeField.REGION: db_repr.OnsPostcodeColumnNames.REGION_ID,
                OnsPostcodeField.WESTMINISTER_PARLIAMENTRY_CONSTITUENCY: db_repr.OnsPostcodeColumnNames.CONSTITUENCY_ID,  # noqa: E501
                OnsPostcodeField.ELECTORAL_WARD: db_repr.OnsPostcodeColumnNames.ELECTORAL_WARD_ID,
            },
            inplace=True,
        )
        rows.dropna(subset=[db_repr.OnsPostcodeColumnNames.CONSTITUENCY_ID], inplace=True)
        rows[db_repr.OnsPostcodeColumnNames.POSTCODE_DISTRICT] = rows.apply(lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][:-3], axis=1)
        rows.to_sql(
            db_repr.OnsPostcode.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            index_label=db_repr.OnsPostcodeColumnNames.POSTCODE,
            chunksize=100000,
        )

        self.logger.info(
            f"Finished parsing ONS postcodes file, wrote {len(rows.index)} items"
        )

    def add_postcode_district_to_add(self):
        rows = pd.read_sql_table(db_repr.OnsPostcode.__tablename__, self.engine)
        rows[db_repr.OnsPostcodeColumnNames.POSTCODE_DISTRICT] = rows.apply(lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][:-3], axis=1)
        rows.to_sql(
            db_repr.OnsPostcode.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            index_label=db_repr.OnsPostcodeColumnNames.POSTCODE,
            chunksize=100000,
        )

    def clear_all(self):
        """Clears all rows from the ONS postcodes table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsPostcode).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)

if __name__ == "__main__":
    config.init_loggers()
    config.parse_config()

    x = PostcodeCsvParser()
    x.add_postcode_district_to_add()
