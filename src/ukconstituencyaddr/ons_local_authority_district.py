"""
Local authorities map

https://geoportal.statistics.gov.uk/datasets/ons::local-authority-districts-december-2023-boundaries-uk-bfe-2/about

https://www.ons.gov.uk/census/maps/choropleth/population/age/resident-age-8c/aged-15-to-24-years

Filtered by local tier local authorities

LOCAL_AUTHORITY_DISTRICT = "laua"
"""


import enum
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from ukconstituencyaddr import ons_constituencies
from ukconstituencyaddr import config
from ukconstituencyaddr.db import cacher
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class OnsLocalAuthorityField(enum.StrEnum):
    """Enum to match fields to headers in the CSV"""

    ENTRY_ID = "FID"
    ID = "LAD23CD"
    NAME = "LAD23NM"
    WARD_NAME = "LAD23NMW"
    BNG_E = "BNG_E"
    BNG_N = "BNG_N"
    LONGITUDE = "LONG"
    LATITUDE = "LAT"
    SHAPE_AREA = "Shape__Area"
    SHAPE_LENGTH = "Shape__Length"
    GLOBAL_ID = "GlobalID"


class LocalAuthorityCsvParser:
    """Reads ONS Postcode CSV data into the database"""

    def __init__(
        self,
    ) -> None:
        self.csv = config.conf.input.ons_local_auth_csv
        if not self.csv.exists():
            raise Exception(f"CSV file not at {self.csv}")

        self.csv_name = cacher.DatafileName.OnsLocalAuthorityDistrict

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(f"Using CSV {self.csv}")

    def process_csv(self):
        """Reads the CSV into the database"""
        modified = cacher.DbCacheInst.check_file_modified(self.csv_name, self.csv)
        if not modified:
            self.logger.info("Already parsed CSV file and placed into db")
            return

        self.logger.info("Parsing ONS local authority district file")

        rows = pd.read_csv(
            self.csv,
            header=0,
            usecols=[
                OnsLocalAuthorityField.ID,
                OnsLocalAuthorityField.NAME,
                OnsLocalAuthorityField.WARD_NAME,
            ],
        )

        rows.rename(
            columns={
                OnsLocalAuthorityField.ID: db_repr.OnsLocalAuthorityColumnsNames.OID,
                OnsLocalAuthorityField.NAME: db_repr.OnsLocalAuthorityColumnsNames.NAME,
                OnsLocalAuthorityField.WARD_NAME: db_repr.OnsLocalAuthorityColumnsNames.WARD_NAME,
            },
            inplace=True,
        )

        rows.to_sql(
            db_repr.OnsLocalAuthorityDistrict.__tablename__,
            self.engine,
            if_exists="append",
            index=False,
            chunksize=100000,
        )

        cacher.DbCacheInst.set_file_modified(self.csv_name, self.csv)

        self.logger.info(
            f"Finished parsing ONS local authority file, wrote {len(rows.index)} items"
        )

    def get_local_authority(
        self, id: str
    ) -> Optional[db_repr.OnsLocalAuthorityDistrict]:
        """
        Returns the constituency specified by the ID
        (which is defined by the ONS) if it exists
        """
        session = Session(self.engine)
        try:
            if len(id) == 0:
                raise ValueError("You must provide a string that isn't empty!")
            else:
                return session.get(db_repr.OnsLocalAuthorityDistrict, id)
        finally:
            session.close()

    def get_local_authority_by_name(
        self, name: str
    ) -> Optional[db_repr.OnsLocalAuthorityDistrict]:
        """Returns the constituency by name if it exists. Only performs exact matches."""
        session = Session(self.engine)
        try:
            if len(name) == 0:
                raise ValueError("You must provide a string that isn't empty!")
            else:
                return (
                    session.query(db_repr.OnsLocalAuthorityDistrict)
                    .filter(db_repr.OnsLocalAuthorityDistrict.name == name)
                    .one()
                )
        finally:
            session.close()

    def clear_all(self):
        """Clears all rows from the ONS local authority district table"""
        with Session(self.engine) as session:
            session.query(db_repr.OnsLocalAuthorityDistrict).delete()
            session.commit()
            cacher.DbCacheInst.clear_file_modified(self.csv_name)


if __name__ == "__main__":
    config.init_loggers()
    config.parse_config()
