import enum
import logging
from datetime import datetime
import pathlib
import threading
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

from ukconstituencyaddr import config

CACHE_DB_FILE = config.MAIN_STORAGE_FOLDER / "local_cache.sqlite"


def get_engine(local_db_filename: pathlib.Path | str = CACHE_DB_FILE) -> Engine:
    engine = create_engine(
        f"sqlite+pysqlite:///{str(local_db_filename)}",
    )
    return engine


DB_THREADING_LOCK = threading.Lock()


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


class ApiUseLogColumnNames(enum.StrEnum):
    MINUTE = "minute"
    NUM_REQUESTS = "num_requests"


class ApiUseLog(Base):
    __tablename__ = "api_use_log"

    minute: Mapped[datetime] = mapped_column(primary_key=True)
    num_requests: Mapped[int]

    def __repr__(self) -> str:
        return self._repr(
            minute=self.minute,
            num_requests=self.num_requests,
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

    oas: Mapped[List["OnsOa"]] = relationship(
        back_populates="local_authority_district", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            oid=self.oid,
            name=self.name,
            ward_name=self.ward_name,
        )


class OnsOaColumnsNames(enum.StrEnum):
    OID = "oid"
    LSOA_ID = "lsoa_id"
    MSOA_ID = "msoa_id"
    LOCAL_AUTH_DISTRICT_ID = "local_auth_district_id"


class OnsOa(Base):
    __tablename__ = "ons_oa"

    oid: Mapped[str] = mapped_column(primary_key=True)
    lsoa_id: Mapped[str]
    msoa_id: Mapped[str] = mapped_column(ForeignKey("ons_msoa.oid"), index=True)
    local_auth_district_id: Mapped[str] = mapped_column(
        ForeignKey("ons_local_auth_district.oid"), index=True
    )

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="oa", lazy="select"
    )

    msoa: Mapped["OnsMsoa"] = relationship(back_populates="oas", lazy="select")

    local_authority_district: Mapped["OnsLocalAuthorityDistrict"] = relationship(
        back_populates="oas", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            oid=self.oid,
            lsoa_id=self.lsoa_id,
            msoa_id=self.msoa_id,
            local_auth_district_id=self.local_auth_district_id,
        )


class OnsMsoaColumnsNames(enum.StrEnum):
    OID = "oid"
    NAME = "name"
    GB_OS_EASTING = "gb_os_easting"
    GB_OS_NORTHING = "gb_os_northing"
    SHAPE_AREA = "shape_area"
    SHAPE_LENGTH = "shape_length"
    GEOMETRY = "geometry"


class OnsMsoa(Base):
    __tablename__ = "ons_msoa"

    oid: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    gb_os_easting: Mapped[int]
    gb_os_northing: Mapped[int]
    shape_area: Mapped[float]
    shape_length: Mapped[float]
    geometry: Mapped[str]

    postcodes: Mapped[List["OnsPostcode"]] = relationship(
        back_populates="msoa", lazy="select"
    )

    oas: Mapped[List["OnsOa"]] = relationship(back_populates="msoa", lazy="select")

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
            msoa_id=self.msoa_id,
            age_range=self.age_range,
            observed_count=self.observed_count,
            percent_of_msoa=self.percent_of_msoa,
        )


class OnsPostcodeColumnNames(enum.StrEnum):
    POSTCODE = "postcode"
    POSTCODE_OUTCODE = "postcode_outcode"  # 'outward code' e.g. of AA9A 9AA, 'AA9A' would be the out code
    POSTCODE_INCODE = "postcode_incode"  # 'inward code' e.g. of AA9A 9AA, '9AA' would be in the in code
    POSTCODE_DISTRICT = (
        "postcode_district"  # e.g. of AA9A 9AA, 'AA9A 9' would be the sector
    )
    POSTCODE_SUBDISTRICT = (
        "postcode_subdistrict"  # e.g. of AA9A 9AA, 'AA9A 9' would be the sector
    )
    POSTCODE_AREA = "postcode_area"  # e.g. of AA9A 9AA, 'AA9A 9' would be the sector
    POSTCODE_SECTOR = (
        "postcode_sector"  # e.g. of AA9A 9AA, 'AA9A 9' would be the sector
    )
    COUNTRY_ID = "country_id"
    REGION_ID = "region_id"
    CONSTITUENCY_ID = "constituency_id"
    ELECTORAL_WARD_ID = "electoral_ward_id"
    LOCAL_AUTHORITY_DISTRICT_ID = "local_authority_district_id"
    OA_ID = "oa_id"
    MSOA_ID = "msoa_id"


class OnsPostcode(Base):
    __tablename__ = "ons_postcode"

    postcode: Mapped[str] = mapped_column(primary_key=True)
    postcode_outcode: Mapped[str] = mapped_column(index=True)
    postcode_incode: Mapped[str] = mapped_column(index=True)
    postcode_sector: Mapped[str] = mapped_column(index=True)
    postcode_district: Mapped[str] = mapped_column(index=True)
    postcode_subdistrict: Mapped[Optional[str]] = mapped_column(index=True)
    postcode_area: Mapped[str] = mapped_column(index=True)
    country_id: Mapped[Optional[str]]
    region_id: Mapped[Optional[str]]
    constituency_id: Mapped[str] = mapped_column(
        ForeignKey("ons_constituency.oid"), index=True
    )
    electoral_ward_id: Mapped[str]
    local_authority_district_id: Mapped[str] = mapped_column(
        ForeignKey("ons_local_auth_district.oid"), index=True
    )
    oa_id: Mapped[str] = mapped_column(ForeignKey("ons_oa.oid"), index=True)
    msoa_id: Mapped[str] = mapped_column(ForeignKey("ons_msoa.oid"), index=True)

    constituency: Mapped["OnsConstituency"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    local_authority: Mapped["OnsLocalAuthorityDistrict"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    oa: Mapped["OnsOa"] = relationship(back_populates="postcodes", lazy="select")

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


class CensusAgeByOaColumnsNames(enum.StrEnum):
    OA_ID = "oa_id"
    AGE_TOTAL = "age_total"
    TOTAL_15_TO_34 = "total_15_to_34"
    PERCENTAGE_15_TO_34 = "percentage_15_to_34"


class CensusAgeByOa(Base):
    __tablename__ = "census_age_by_oa"

    oa_id: Mapped[str] = mapped_column(
        ForeignKey("ons_oa.oid"),
        primary_key=True,
    )
    age_total: Mapped[int]
    total_15_to_34: Mapped[int]
    percentage_15_to_34: Mapped[float]

    oa: Mapped["OnsOa"] = relationship()

    def __repr__(self) -> str:
        return self._repr(
            oa_id=self.oa_id,
            age_total=self.age_total,
            total_15_to_34=self.total_15_to_34,
            percentage_15_to_34=self.percentage_15_to_34,
        )


class OsOpennameRoadColumnNames(enum.StrEnum):
    OS_ID = "os_id"
    NAME = "name"
    LOCAL_TYPE = "local_type"
    POSTCODE_DISTRICT = "postcode_district"
    POPULATED_PLACE = "populated_place"
    GB_OS_EASTING = "gb_os_easting"
    GB_OS_NORTHING = "gb_os_northing"

    # Minimum bounding rectangle
    MBR_XMIN = "mbr_xmin"
    MBR_XMAX = "mbr_xmax"
    MBR_YMIN = "mbr_ymin"
    MBR_YMAX = "mbr_ymax"


class OsOpennameRoad(Base):
    __tablename__ = "os_openname_road"

    os_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    local_type: Mapped[str]
    postcode_district: Mapped[str] = mapped_column(
        ForeignKey("ons_postcode.postcode_district"), index=True
    )
    populated_place: Mapped[Optional[str]]
    gb_os_easting: Mapped[int]
    gb_os_northing: Mapped[int]

    # Minimum bounding rectangle
    mbr_xmin: Mapped[int]
    mbr_xmax: Mapped[int]
    mbr_ymin: Mapped[int]
    mbr_ymax: Mapped[int]

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
    postcode = mapped_column(ForeignKey("ons_postcode.postcode"), index=True)

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
        return self._repr(postcode=self.postcode, was_fetched=self.was_fetched)
