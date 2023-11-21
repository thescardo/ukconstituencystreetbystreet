import enum
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

from sqlalchemy import Engine, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    exc,
    mapped_column,
    relationship,
)

from ukconstituencyaddr.config import MAIN_STORAGE_FOLDER

CACHE_DB_FILE = MAIN_STORAGE_FOLDER / "local_cache.sqlite"


def get_engine() -> Engine:
    engine = create_engine(
        f"sqlite+pysqlite:///{str(CACHE_DB_FILE)}",
    )
    return engine


class Cacher(Protocol):
    @property
    def engine(self) -> Engine:
        pass

    @property
    def logger(self) -> logging.Logger:
        pass

    @property
    def session(self) -> Session:
        pass


def wrap_session(func):
    def magic(self: Cacher, *args, **kwargs):
        self.session = Session(self.engine)
        ret = func(self, *args, **kwargs)
        self.session.close()
        return ret

    return magic


class Base(DeclarativeBase):
    def _repr(self, **fields: Dict[str, Any]) -> str:
        """
        Helper for __repr__
        """
        field_strings = []
        at_least_one_attached_attribute = False
        for key, field in fields.items():
            try:
                field_strings.append(f"{key}={field!r}")
            except exc.DetachedInstanceError:
                field_strings.append(f"{key}=DetachedInstanceError")
            else:
                at_least_one_attached_attribute = True
        if at_least_one_attached_attribute:
            return f"<{self.__class__.__name__}({','.join(field_strings)})>"
        return f"<{self.__class__.__name__} {id(self)}>"


class CsvFilesModified(Base):
    """User class will be converted to a dataclass"""

    __tablename__ = "csv_files_modified"

    name: Mapped[str] = mapped_column(primary_key=True)
    filename: Mapped[str]
    modified: Mapped[datetime]

    def __repr__(self) -> str:
        return self._repr(
            name=self.name, filename=self.filename, modified=self.modified
        )


class OnsConstituency(Base):
    __tablename__ = "ons_constituency"

    id: Mapped[str] = mapped_column(String(9), primary_key=True)
    name: Mapped[str]

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="constituency", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(id=self.id, name=self.name)


class OnsPostcodeColumnNames(enum.StrEnum):
    POSTCODE = "postcode"
    POSTCODE_DISTRICT = "postcode_district"
    COUNTRY_ID = "country_id"
    REGION_ID = "region_id"
    CONSTITUENCY_ID = "constituency_id"
    ELECTORAL_WARD_ID = "electoral_ward_id"


class OnsPostcode(Base):
    __tablename__ = "ons_postcode"

    postcode: Mapped[str] = mapped_column(
        String(7), primary_key=True, name=OnsPostcodeColumnNames.POSTCODE
    )
    postcode_district: Mapped[str] = mapped_column(
        String(4), index=True, name=OnsPostcodeColumnNames.POSTCODE_DISTRICT
    )
    country_id: Mapped[Optional[str]] = mapped_column(
        String(9), name=OnsPostcodeColumnNames.COUNTRY_ID
    )
    region_id: Mapped[Optional[str]] = mapped_column(
        String(9), name=OnsPostcodeColumnNames.REGION_ID
    )
    constituency_id: Mapped[str] = mapped_column(
        ForeignKey("ons_constituency.id"), name=OnsPostcodeColumnNames.CONSTITUENCY_ID
    )
    electoral_ward_id: Mapped[str] = mapped_column(
        String(9), name=OnsPostcodeColumnNames.ELECTORAL_WARD_ID
    )

    constituency: Mapped["OnsConstituency"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    addresses: Mapped[List["SimpleAddress"]] = relationship(
        back_populates="ons_postcode", lazy="select"
    )

    roads: Mapped[List["OsOpennameRoad"]] = relationship(
        back_populates="ons_postcodes", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            postcode=self.postcode,
            country_id=self.country_id,
            region_id=self.region_id,
            constituency=self.constituency,
            electoral_ward_id=self.electoral_ward_id,
        )


class OsOpennameRoadColumnNames(enum.StrEnum):
    OS_ID = "os_id"
    NAME = "name"
    LOCAL_TYPE = "local_type"
    POSTCODE_DISTRICT = "postcode_district"
    POPULATED_PLACE = "populated_place"


class OsOpennameRoad(Base):
    __tablename__ = "os_openname_road"

    os_id: Mapped[str] = mapped_column(
        String, primary_key=True, name=OsOpennameRoadColumnNames.OS_ID
    )
    name: Mapped[str] = mapped_column(String, name=OsOpennameRoadColumnNames.NAME)
    local_type: Mapped[str] = mapped_column(
        String, name=OsOpennameRoadColumnNames.LOCAL_TYPE
    )
    postcode_district: Mapped[str] = mapped_column(
        ForeignKey("ons_postcode.postcode_district"), name=OsOpennameRoadColumnNames.POSTCODE_DISTRICT
    )
    populated_place: Mapped[Optional[str]] = mapped_column(
        String, name=OsOpennameRoadColumnNames.POPULATED_PLACE
    )

    ons_postcodes: Mapped[List[OnsPostcode]] = relationship(
        back_populates="roads", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            os_id=self.os_id,
            name=self.name,
            local_type=self.local_type,
            postcode_district=self.postcode_district,
            populated_place=self.populated_place,
        )


class SimpleAddressColumnNames(enum.StrEnum):
    GET_ADDRESS_IO_ID = "get_address_io_id"
    HOUSE_NUM_OR_NAME = "house_num_or_name"
    LINE_1 = "line_1"
    LINE_2 = "line_2"
    LINE_3 = "line_3"
    LINE_4 = "line_4"
    THOROUGHFARE_OR_DESC = "thoroughfare_or_desc"
    TOWN_OR_CITY = "town_or_city"
    LOCALITY = "locality"
    COUNTY = "county"
    COUNTRY = "country"
    POSTCODE = "postcode"


class SimpleAddress(Base):
    __tablename__ = "simple_addresses"

    get_address_io_id: Mapped[str] = mapped_column(
        String, name=SimpleAddressColumnNames.GET_ADDRESS_IO_ID, primary_key=True
    )
    house_num_or_name: Mapped[Optional[str]] = mapped_column(
        String, name=SimpleAddressColumnNames.HOUSE_NUM_OR_NAME
    )
    line_1: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.LINE_1)
    line_2: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.LINE_2)
    line_3: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.LINE_3)
    line_4: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.LINE_4)
    thoroughfare_or_desc: Mapped[str] = mapped_column(
        String, name=SimpleAddressColumnNames.THOROUGHFARE_OR_DESC
    )
    town_or_city: Mapped[str] = mapped_column(
        String, name=SimpleAddressColumnNames.TOWN_OR_CITY
    )
    locality: Mapped[str] = mapped_column(
        String, name=SimpleAddressColumnNames.LOCALITY
    )
    county: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.COUNTY)
    country: Mapped[str] = mapped_column(String, name=SimpleAddressColumnNames.COUNTRY)
    postcode = mapped_column(
        ForeignKey("ons_postcode.postcode"), name=SimpleAddressColumnNames.POSTCODE
    )

    ons_postcode: Mapped[OnsPostcode] = relationship(
        back_populates="addresses", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            id=self.id,
            postcode=self.postcode,
            line_1=self.line_1,
            line_2=self.line_2,
            line_3=self.line_3,
            line_4=self.line_4,
            thoroughfare_or_desc=self.thoroughfare_or_desc,
            town_or_city=self.town_or_city,
            locality=self.locality,
            county=self.county,
            country=self.country,
        )


class PostcodeFetchedNames(enum.StrEnum):
    POSTCODE = "postcode"
    WAS_FETCHED = "was_fetched"
    CONSTITUENCY_ID = "constituency_id"


class PostcodeFetched(Base):
    __tablename__ = "postcode_fetched"
    __table_args__ = {"sqlite_autoincrement": True}

    postcode = mapped_column(
        ForeignKey("ons_postcode.postcode"),
        primary_key=True,
        name=PostcodeFetchedNames.POSTCODE,
    )
    constituency_id: Mapped[str] = mapped_column(
        ForeignKey("ons_constituency.id"), name=PostcodeFetchedNames.CONSTITUENCY_ID
    )
    was_fetched = mapped_column(String, name=PostcodeFetchedNames.WAS_FETCHED)

    def __repr__(self) -> str:
        return self._repr(
            id=self.id, postcode=self.postcode, was_fetched=self.was_fetched
        )
