import enum
import logging
import os.path
import pathlib
from datetime import datetime

from sqlalchemy.orm import Session

from .db_repr_sqlite import (
    Base,
    CsvFilesModified,
    get_engine,
    wrap_session,
)


class DatafileName(enum.StrEnum):
    OnsConstituency = "ons_constituency_csv"
    OnsPostcode = "ons_postcode_csv"
    RoyalMailPaf = "royal_mail_paf_csv"
    OsOpennamesRoad = "os_opennames_roads"
    OnsLocalAuthorityDistrict = "ons_local_auth_csv"
    OnsMsoaCsv = "ons_msoa_csv"
    OnsMsoaGeoJson = "ons_msoa_geojson"
    OnsMsoaReadableNames = "ons_msoa_readable_csv"
    OnsOa = "ons_oa_csv"
    CensusAgeByMsoa = "census_age_by_msoa_csv"
    CensusAgeByOa = "census_age_by_oa_csv"


class DbCache:
    """Stores how recently the CSV in the database has been modified"""

    def __init__(self) -> None:
        self.engine = get_engine()
        self.session = Session(self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.debug("Created class")

        Base.metadata.create_all(bind=self.engine)

    @wrap_session
    def check_file_modified(self, file_id: DatafileName, file: pathlib.Path) -> bool:
        self.logger.debug("Checking file modified time of file_id")
        row = self.session.get(CsvFilesModified, file_id.value)
        modified_time = datetime.fromtimestamp(os.path.getmtime(file))

        if row is None:
            self.logger.debug(f"No row found for {file_id=} {file=}")
            return True

        if row.filename != str(file) or row.modified != modified_time:
            self.logger.debug(f"File has been modified {file_id=} {file=}")
            return True

        self.logger.debug(f"File has not been modified {file_id=} {file=}")
        return False

    def set_file_modified(self, file_id: DatafileName, file: pathlib.Path) -> None:
        self.logger.debug("Setting file modified time of file_id")
        row = self.session.get(CsvFilesModified, file_id.value)
        modified_time = datetime.fromtimestamp(os.path.getmtime(file))

        if row is None:
            self.session.add(
                CsvFilesModified(
                    name=file_id.value, filename=str(file), modified=modified_time
                )
            )
        else:
            row.filename = str(file)
            row.modified = modified_time

        self.session.flush()
        self.session.commit()

    def check_and_set_file_modified(
        self, file_id: DatafileName, file: pathlib.Path
    ) -> bool:
        check = self.check_file_modified(file_id, file)
        self.set_file_modified(file_id, file)
        return check

    @wrap_session
    def clear_file_modified(self, file_id: DatafileName):
        self.session.query(CsvFilesModified).filter_by(name=file_id.value).delete()
        self.session.commit()

    @wrap_session
    def clear_all(self):
        self.session.query(CsvFilesModified).delete()
        self.session.commit()


DbCacheInst = DbCache()
