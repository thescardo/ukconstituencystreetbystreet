import enum
import logging
import os.path
import pathlib
from datetime import datetime

from sqlalchemy.orm import Session

from .db_repr_sqlite import (
    Base,
    CsvFilesModified,
    OnsConstituency,
    OnsPostcode,
    RoyalMailPaf,
    get_engine,
    wrap_session,
)


class CsvName(enum.StrEnum):
    OnsConstituency = "ons_constituency_csv"
    OnsPostcode = "ons_postcode_csv"
    RoyalMailPaf = "royal_mail_paf_csv"


class DbCache:
    def __init__(self) -> None:
        self.engine = get_engine()
        self.session = Session(self.engine)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.debug("Created class")

        Base.metadata.create_all(bind=self.engine)

    @wrap_session
    def check_and_set_file_modified(self, file_id: CsvName, file: pathlib.Path) -> bool:
        self.logger.debug("Checking file modified time of file_id")
        row = self.session.get(CsvFilesModified, file_id.value)
        modified_time = datetime.fromtimestamp(os.path.getmtime(file))
        if row is None:
            self.logger.debug(f"No row found for {file_id=} {file=}")
            self.session.add(
                CsvFilesModified(
                    name=file_id.value, filename=str(file), modified=modified_time
                )
            )
            self.session.flush()
            self.session.commit()
            return True

        if row.filename != str(file):
            row.filename = str(file)
        if row.modified != modified_time:
            row.modified = modified_time

        if row in self.session.dirty:
            self.session.flush()
            self.session.commit()
            self.logger.debug(f"File has been modified {file_id=} {file=}")
            return True
        else:
            self.logger.debug(f"File has not been modified {file_id=} {file=}")
            return False

    @wrap_session
    def clear_file_modified(self, file_id: CsvName):
        self.session.query(CsvFilesModified).filter_by(name=file_id.value).delete()
        self.session.commit()

    @wrap_session
    def clear_all(self):
        self.session.query(CsvFilesModified).delete()
        self.session.commit()


DbCacheInst = DbCache()
