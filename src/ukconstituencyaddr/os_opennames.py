"""
Imports OS Open Names data and pushes it into the configured database.

See https://osdatahub.os.uk/downloads/open/OpenNames, 'A comprehensive dataset of place names, roads numbers and postcodes for Great Britain.'
"""

import enum
import logging
import pathlib
from typing import List

import pandas as pd
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class OsOpennamesFields(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    ID = "ID"
    NAMES_URI = "NAMES_URI"
    NAME1 = "NAME1"
    NAME1_LANG = "NAME1_LANG"
    NAME2 = "NAME2"
    NAME2_LANG = "NAME2_LANG"
    TYPE = "TYPE"
    LOCAL_TYPE = "LOCAL_TYPE"
    GB_OS_EASTING = "GEOMETRY_X"
    GB_OS_NORTHING = "GEOMETRY_Y"
    MOST_DETAIL_VIEW_RES = "MOST_DETAIL_VIEW_RES"
    LEAST_DETAIL_VIEW_RES = "LEAST_DETAIL_VIEW_RES"
    MBR_XMIN = "MBR_XMIN"
    MBR_YMIN = "MBR_YMIN"
    MBR_XMAX = "MBR_XMAX"
    MBR_YMAX = "MBR_YMAX"
    POSTCODE_DISTRICT = "POSTCODE_DISTRICT"
    POSTCODE_DISTRICT_URI = "POSTCODE_DISTRICT_URI"
    POPULATED_PLACE = "POPULATED_PLACE"
    POPULATED_PLACE_URI = "POPULATED_PLACE_URI"
    POPULATED_PLACE_TYPE = "POPULATED_PLACE_TYPE"
    DISTRICT_BOROUGH = "DISTRICT_BOROUGH"
    DISTRICT_BOROUGH_URI = "DISTRICT_BOROUGH_URI"
    DISTRICT_BOROUGH_TYPE = "DISTRICT_BOROUGH_TYPE"
    COUNTY_UNITARY = "COUNTY_UNITARY"
    COUNTY_UNITARY_URI = "COUNTY_UNITARY_URI"
    COUNTY_UNITARY_TYPE = "COUNTY_UNITARY_TYPE"
    REGION = "REGION"
    REGION_URI = "REGION_URI"
    COUNTRY = "COUNTRY"
    COUNTRY_URI = "COUNTRY_URI"
    RELATED_SPATIAL_OBJECT = "RELATED_SPATIAL_OBJECT"
    SAME_AS_DBPEDIA = "SAME_AS_DBPEDIA"
    SAME_AS_GEONAMES = "SAME_AS_GEONAMES"


class OsOpenNamesCsvsParser:
    """Reads OS Opennames CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv_folder = config.conf.input.os_openname_csv_folder
        if not self.csv_folder.exists() or not self.csv_folder.is_dir():
            raise Exception(f"CSV file not at {self.csv_folder}")

        self.csv_files: List[pathlib.Path] = list(self.csv_folder.glob("*.csv"))
        if len(self.csv_files) == 0:
            raise Exception(f"Unable to find any CSVs in {self.csv_folder}")

        self.csv_name = cacher.DatafileName.OsOpennamesRoad

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Using CSV folder {self.csv_folder}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(
            self.csv_name, self.csv_folder
        )
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing OS opennames files")

        def strip_spaces(x: str):
            return x.replace(" ", "")

        all_csvs = []
        for file in self.csv_files:
            all_csvs.append(
                pd.read_csv(
                    file,
                    header=0,
                    names=list(OsOpennamesFields),
                    converters={
                        OsOpennamesFields.POSTCODE_DISTRICT: strip_spaces,
                    },
                    usecols=[
                        OsOpennamesFields.ID,
                        OsOpennamesFields.NAME1,
                        OsOpennamesFields.LOCAL_TYPE,
                        OsOpennamesFields.POSTCODE_DISTRICT,
                        OsOpennamesFields.POPULATED_PLACE,
                        OsOpennamesFields.GB_OS_EASTING,
                        OsOpennamesFields.GB_OS_NORTHING,
                        OsOpennamesFields.MBR_XMIN,
                        OsOpennamesFields.MBR_XMAX,
                        OsOpennamesFields.MBR_YMIN,
                        OsOpennamesFields.MBR_YMAX,
                    ],
                )
            )

        rows = pd.concat(all_csvs)

        rows = rows[rows[OsOpennamesFields.LOCAL_TYPE].str.contains("Road")]

        # Convert to integer types after we've removed everything that might not have area etc
        rows[OsOpennamesFields.GB_OS_EASTING] = rows[
            OsOpennamesFields.GB_OS_EASTING
        ].astype(pd.Int64Dtype())
        rows[OsOpennamesFields.GB_OS_NORTHING] = rows[
            OsOpennamesFields.GB_OS_NORTHING
        ].astype(pd.Int64Dtype())
        rows[OsOpennamesFields.MBR_XMIN] = rows[OsOpennamesFields.MBR_XMIN].astype(
            pd.Int64Dtype()
        )
        rows[OsOpennamesFields.MBR_XMAX] = rows[OsOpennamesFields.MBR_XMAX].astype(
            pd.Int64Dtype()
        )
        rows[OsOpennamesFields.MBR_YMIN] = rows[OsOpennamesFields.MBR_YMIN].astype(
            pd.Int64Dtype()
        )
        rows[OsOpennamesFields.MBR_YMAX] = rows[OsOpennamesFields.MBR_YMAX].astype(
            pd.Int64Dtype()
        )

        rows.rename(
            columns={
                OsOpennamesFields.ID: db_repr.OsOpennameRoadColumnNames.OS_ID,
                OsOpennamesFields.NAME1: db_repr.OsOpennameRoadColumnNames.NAME,
                OsOpennamesFields.LOCAL_TYPE: db_repr.OsOpennameRoadColumnNames.LOCAL_TYPE,
                OsOpennamesFields.POSTCODE_DISTRICT: db_repr.OsOpennameRoadColumnNames.POSTCODE_DISTRICT,
                OsOpennamesFields.POPULATED_PLACE: db_repr.OsOpennameRoadColumnNames.POPULATED_PLACE,
                OsOpennamesFields.GB_OS_EASTING: db_repr.OsOpennameRoadColumnNames.GB_OS_EASTING,
                OsOpennamesFields.GB_OS_NORTHING: db_repr.OsOpennameRoadColumnNames.GB_OS_NORTHING,
                OsOpennamesFields.MBR_XMIN: db_repr.OsOpennameRoadColumnNames.MBR_XMIN,
                OsOpennamesFields.MBR_XMAX: db_repr.OsOpennameRoadColumnNames.MBR_XMAX,
                OsOpennamesFields.MBR_YMIN: db_repr.OsOpennameRoadColumnNames.MBR_YMIN,
                OsOpennamesFields.MBR_YMAX: db_repr.OsOpennameRoadColumnNames.MBR_YMAX,
            },
            inplace=True,
        )

        rows.to_sql(
            db_repr.OsOpennameRoad.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        self.logger.info(
            f"Finished parsing ONS postcodes file, wrote {len(rows.index)} items"
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv_folder)

    def clear_all(self):
        """Clears all rows from OS open names table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsPostcode).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)
