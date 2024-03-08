import enum
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

from sqlalchemy import Engine, Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    exc,
    mapped_column,
    relationship,
)
from sqlalchemy.ext.declarative import declarative_base

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


class OnsConstituencyColumnsNames(enum.StrEnum):
    OID = "oid"
    NAME = "name"


class OnsConstituency(Base):
    __tablename__ = "ons_constituency"

    oid: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="constituency", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(oid=self.oid, name=self.name)


class OnsLocalAuthorityColumnsNames(enum.StrEnum):
    OID = "oid"
    NAME = "name"
    WARD_NAME = "ward_name"


class OnsLocalAuthorityDistrict(Base):
    __tablename__ = "ons_local_auth_district"

    oid: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    ward_name: Mapped[str]

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="local_authority", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            oid=self.oid,
            name=self.name,
            ward_name=self.ward_name,
        )


class OnsMsoaColumnsNames(enum.StrEnum):
    OID = "oid"
    NAME = "name"


class OnsMsoa(Base):
    __tablename__ = "ons_msoa"

    oid: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="msoa", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            oid=self.oid,
            name=self.name,
        )


class CensusAgeByMsoaColumnsNames(enum.StrEnum):
    MSOA_ID = "msoa_id"
    AGE_RANGE = "age_range"
    OBSERVED_COUNT = "observed_count"
    PERCENT_OF_MSOA = "percent_of_msoa"


class CensusAgeRange(enum.StrEnum):
    R_0_15 = "0-15"
    R_16_35 = "16-35"
    R_36_100 = "36-100+"


class CensusAgeByMsoa(Base):
    __tablename__ = "census_age_by_msoa"

    msoa_id: Mapped[str] = mapped_column(
        ForeignKey("ons_msoa.oid"),
        primary_key=True,
    )
    age_range: Mapped[int] = mapped_column(primary_key=True)
    observed_count: Mapped[int]
    percent_of_msoa: Mapped[float]

    msoa: Mapped["OnsMsoa"] = relationship()

    def __repr__(self) -> str:
        return self._repr(
            id=self.msoa_id,
            name=self.age_range,
            observed_count=self.observed_count,
            percent_of_msoa=self.percent_of_msoa,
        )


class OnsPostcodeColumnNames(enum.StrEnum):
    POSTCODE = "postcode"
    POSTCODE_DISTRICT = "postcode_district"
    COUNTRY_ID = "country_id"
    REGION_ID = "region_id"
    CONSTITUENCY_ID = "constituency_id"
    ELECTORAL_WARD_ID = "electoral_ward_id"
    LOCAL_AUTHORITY_DISTRICT_ID = "local_authority_district_id"
    MSOA_ID = "msoa_id"


class OnsPostcode(Base):
    __tablename__ = "ons_postcode"

    postcode: Mapped[str] = mapped_column(primary_key=True)
    postcode_district: Mapped[str] = mapped_column(index=True)
    country_id: Mapped[Optional[str]]
    region_id: Mapped[Optional[str]]
    constituency_id: Mapped[str] = mapped_column(ForeignKey("ons_constituency.oid"))
    electoral_ward_id: Mapped[str]
    local_authority_district_id: Mapped[str] = mapped_column(
        ForeignKey("ons_local_auth_district.oid"),
    )
    msoa_id: Mapped[str] = mapped_column(ForeignKey("ons_msoa.oid"))

    constituency: Mapped["OnsConstituency"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    local_authority: Mapped["OnsLocalAuthorityDistrict"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    msoa: Mapped["OnsMsoa"] = relationship(back_populates="postcodes", lazy="select")

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

    os_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    local_type: Mapped[str]
    postcode_district: Mapped[str] = mapped_column(
        ForeignKey("ons_postcode.postcode_district"),
    )
    populated_place: Mapped[Optional[str]]

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

    get_address_io_id: Mapped[str] = mapped_column(primary_key=True)
    house_num_or_name: Mapped[Optional[str]]
    line_1: Mapped[Optional[str]]
    line_2: Mapped[Optional[str]]
    line_3: Mapped[Optional[str]]
    line_4: Mapped[Optional[str]]
    thoroughfare_or_desc: Mapped[Optional[str]]
    town_or_city: Mapped[Optional[str]]
    locality: Mapped[Optional[str]]
    county: Mapped[Optional[str]]
    country: Mapped[Optional[str]]
    postcode = mapped_column(ForeignKey("ons_postcode.postcode"))

    ons_postcode: Mapped[OnsPostcode] = relationship(
        back_populates="addresses", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            oid=self.get_address_io_id,
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
    )
    constituency_id: Mapped[str] = mapped_column(ForeignKey("ons_constituency.oid"))
    was_fetched: Mapped[bool] = mapped_column()

    def __repr__(self) -> str:
        return self._repr(
            id=self.id, postcode=self.postcode, was_fetched=self.was_fetched
        )
