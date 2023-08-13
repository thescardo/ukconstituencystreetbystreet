import enum
import logging
import pathlib

import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

import ons_constituencies
from db import cacher
from db import db_repr_sqlite as db_repr

ONS_POSTCODE_CSV = pathlib.Path(
    "/home/the/Workspace/GNDR/postcode_lookup/ONS Data/NSPL21_FEB_2023_UK/Data/NSPL21_FEB_2023_UK.csv"
)


class OnsPostcodeField(enum.StrEnum):
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
    def __init__(
        self,
        constituencies: ons_constituencies.ConstituencyCsvParser,
        ons_postcode_csv: pathlib.Path = ONS_POSTCODE_CSV,
    ) -> None:
        self.csv = ons_postcode_csv
        assert self.csv.exists()

        self.csv_name = cacher.CsvName.OnsPostcode
        self.constituencies = constituencies

        self.cache_db_file, self.engine = db_repr.get_engine()
        self.session: Session

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    @db_repr.wrap_session
    def process_csv(self):
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
                OnsPostcodeField.POSTCODE: "postcode",
                OnsPostcodeField.COUNTRY: "country_id",
                OnsPostcodeField.REGION: "region_id",
                OnsPostcodeField.WESTMINISTER_PARLIAMENTRY_CONSTITUENCY: "constituency_id",
                OnsPostcodeField.ELECTORAL_WARD: "electoral_ward_id",
            },
            inplace=True,
        )
        rows.dropna(subset=["constituency_id"], inplace=True)
        rows.to_sql(
            db_repr.OnsPostcode.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            index_label="postcode",
            chunksize=100000,
        )

        self.logger.info(
            f"Finished parsing ONS postcodes file, wrote {len(rows.index)} items"
        )

    @db_repr.wrap_session
    def clear_all(self):
        self.session.query(db_repr.OnsPostcode).delete()
        self.session.commit()
        cacher.DbCacheInst.clear_file_modified(self.csv_name)
