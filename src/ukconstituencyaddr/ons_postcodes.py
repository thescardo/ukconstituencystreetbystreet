"""
Imports ONS postcode data and pushes it into the configured database.

See https://geoportal.statistics.gov.uk/. This module parses National Statistics Postcode Lookup (NSPL) - 2021 Census (February 2024) data.
"""

import enum
import logging
import multiprocessing
import re
from typing import Tuple
import numpy as np

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


OUTCODE_REGEX = r"(?P<sub_district>(?P<district_1>(?P<area_1>[A-Z]{1,2})[0-9]{1})[A-Z]{1})|(?P<district_2>(?P<area_2>[A-Z]{1,2})[0-9]{1,2})|(?P<district_3>[A-Z]{3,4})"


def strip_spaces(x: str):
    return x.replace(" ", "")


def split_postcode(outcode: str) -> Tuple[str, str, str]:
    """
    Splits an outcode (for example, from AA9A 9AA, AA9A would be the outcode)
    into area, district and subdistrict
    """
    match = re.search(OUTCODE_REGEX, outcode)
    if match is None:
        raise ValueError(f"Couldn't find match in '{outcode}'!")

    sub_district = match.group("sub_district")
    district = match.group("district_1")
    if district is None or len(district) == 0:
        district = match.group("district_2")
    if district is None or len(district) == 0:
        district = match.group("district_3")
    area = match.group("area_1")
    if area is None or len(area) == 0:
        area = match.group("area_2")

    if sub_district is None:
        sub_district = ""
    if area is None:
        area = ""
    return area, district, sub_district


def breakdown_postcode(rows: pd.DataFrame) -> pd.DataFrame:
    # Break down postcodes into components

    # OUTCODE is the first 2-4 characters. Since the outcode is always 3 characters
    # long we can just remove the last 3 characters and get it
    rows[db_repr.OnsPostcodeColumnNames.POSTCODE_OUTCODE] = rows.apply(
        lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][:-3], axis=1
    )

    # INCODE is always the last 3 characters
    rows[db_repr.OnsPostcodeColumnNames.POSTCODE_INCODE] = rows.apply(
        lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][-3:], axis=1
    )

    # SECTOR is the OUTCODE plus the first character of the INCODE, so just remove the last
    # two characters
    rows[db_repr.OnsPostcodeColumnNames.POSTCODE_SECTOR] = rows.apply(
        lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][:-2], axis=1
    )

    rows[
        [
            db_repr.OnsPostcodeColumnNames.POSTCODE_AREA,
            db_repr.OnsPostcodeColumnNames.POSTCODE_DISTRICT,
            db_repr.OnsPostcodeColumnNames.POSTCODE_SUBDISTRICT,
        ]
    ] = rows.apply(
        lambda x: split_postcode(x[db_repr.OnsPostcodeColumnNames.POSTCODE_OUTCODE]),
        axis="columns",
        result_type="expand",
    )

    return rows


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
        self.csv = config.conf.input.ons_postcodes_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.DatafileName.OnsPostcode

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(self.csv_name, self.csv)
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing ONS postcodes file")

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
                OnsPostcodeField.LOCAL_AUTHORITY_DISTRICT,
                OnsPostcodeField.OUTPUT_AREA_CENSUS_21,
                OnsPostcodeField.ML_SUPER_OUTPUT_AREA_CENSUS_21,
            ],
        )

        rows.rename(
            columns={
                OnsPostcodeField.POSTCODE: db_repr.OnsPostcodeColumnNames.POSTCODE,
                OnsPostcodeField.COUNTRY: db_repr.OnsPostcodeColumnNames.COUNTRY_ID,
                OnsPostcodeField.REGION: db_repr.OnsPostcodeColumnNames.REGION_ID,
                OnsPostcodeField.WESTMINISTER_PARLIAMENTRY_CONSTITUENCY: db_repr.OnsPostcodeColumnNames.CONSTITUENCY_ID,  # noqa: E501
                OnsPostcodeField.ELECTORAL_WARD: db_repr.OnsPostcodeColumnNames.ELECTORAL_WARD_ID,
                OnsPostcodeField.LOCAL_AUTHORITY_DISTRICT: db_repr.OnsPostcodeColumnNames.LOCAL_AUTHORITY_DISTRICT_ID,
                OnsPostcodeField.OUTPUT_AREA_CENSUS_21: db_repr.OnsPostcodeColumnNames.OA_ID,
                OnsPostcodeField.ML_SUPER_OUTPUT_AREA_CENSUS_21: db_repr.OnsPostcodeColumnNames.MSOA_ID,
            },
            inplace=True,
        )
        rows.dropna(
            subset=[db_repr.OnsPostcodeColumnNames.CONSTITUENCY_ID], inplace=True
        )

        list_df = np.array_split(rows, multiprocessing.cpu_count())
        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            data = pool.map(breakdown_postcode, list_df)
        final_rows = pd.concat(data)

        final_rows.to_sql(
            db_repr.OnsPostcode.__tablename__,
            self.engine,
            index=False,
            if_exists="append",
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(
            f"Finished parsing ONS postcodes file, wrote {len(rows.index)} items"
        )

    def add_postcode_district_to_add(self):
        rows = pd.read_sql_table(db_repr.OnsPostcode.__tablename__, self.engine)
        rows[db_repr.OnsPostcodeColumnNames.POSTCODE_DISTRICT] = rows.apply(
            lambda x: x[db_repr.OnsPostcodeColumnNames.POSTCODE][:-3], axis=1
        )
        rows.to_sql(
            db_repr.OnsPostcode.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
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
