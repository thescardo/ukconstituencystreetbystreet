import enum
import logging
import pathlib
from datetime import datetime
from typing import List, Optional, Protocol, Tuple, Any, Dict

from sqlalchemy import Engine, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Session,
    mapped_column,
    relationship,
    exc,
)


def get_engine() -> Tuple[pathlib.Path, Engine]:
    cache_folder = pathlib.Path("streetcache").absolute().resolve()
    cache_folder.mkdir(parents=True, exist_ok=True)
    cache_db_file = cache_folder / "cache.sqlite"
    engine = create_engine(
        f"sqlite+pysqlite:///{str(cache_db_file)}",
    )
    return cache_db_file, engine


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


class OnsPostcode(Base):
    __tablename__ = "ons_postcode"

    postcode: Mapped[str] = mapped_column(String(7), primary_key=True)
    country_id: Mapped[Optional[str]] = mapped_column(String(9))
    region_id: Mapped[Optional[str]] = mapped_column(String(9))
    constituency_id: Mapped[str] = mapped_column(ForeignKey("ons_constituency.id"))
    electoral_ward_id: Mapped[str] = mapped_column(String(9))

    constituency: Mapped["OnsConstituency"] = relationship(
        back_populates="postcodes", lazy="select"
    )

    addresses: Mapped[List["RoyalMailPaf"]] = relationship(
        back_populates="ons_postcode", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            postcode=self.postcode,
            country_id=self.country_id,
            region_id=self.region_id,
            constituency=self.constituency,
            electoral_ward_id=self.electoral_ward_id,
        )


class PafColumnNames(enum.StrEnum):
    ID = "id"
    POSTCODE = "postcode"
    POST_TOWN = "post_town"
    DEPENDENT_LOCALITY = "dependent_locality"
    DOUBLE_DEPENDENT_LOCALITY = "double_dependent_locality"
    THOROUGHFARE_AND_DESC = "thoroughfare_and_desc"
    DOUBLE_THOROUGHFARE_AND_DESC = "double_thoroughfare_and_desc"
    BUILDING_NUM = "building_num"
    BUILDING_NAME = "building_name"
    SUB_BUILDING_NAME = "sub_building_name"
    PO_BOX = "po_box"
    DEPARTMENT_NAME = "department_name"
    ORG_NAME = "org_name"
    UDPRN = "udprn"
    POSTCODE_TYPE = "postcode_type"
    SU_ORG_IND = "su_org_ind"
    DELIVERY_POINT_SUFFIX = "delivery_point_suffix"
    ADDR_KEY = "addr_key"
    ORG_KEY = "org_key"
    NUM_HOUSEHOLDS = "num_households"
    LOCALITY_KEY = "locality_key"


class RoyalMailPaf(Base):
    __tablename__ = "royal_mail_paf"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(primary_key=True, name=PafColumnNames.ID)
    postcode = mapped_column(
        ForeignKey("ons_postcode.postcode"), name=PafColumnNames.POSTCODE
    )
    post_town: Mapped[str] = mapped_column(String(30), name=PafColumnNames.POST_TOWN)
    dependent_locality: Mapped[Optional[str]] = mapped_column(
        String(35), name=PafColumnNames.DEPENDENT_LOCALITY
    )
    double_dependent_locality: Mapped[Optional[str]] = mapped_column(
        String(35), name=PafColumnNames.DOUBLE_DEPENDENT_LOCALITY
    )
    thoroughfare_and_desc: Mapped[Optional[str]] = mapped_column(
        String(80), name=PafColumnNames.THOROUGHFARE_AND_DESC
    )
    double_thoroughfare_and_desc: Mapped[Optional[str]] = mapped_column(
        String(80), name=PafColumnNames.DOUBLE_THOROUGHFARE_AND_DESC
    )
    building_num: Mapped[Optional[int]] = mapped_column(
        Integer, name=PafColumnNames.BUILDING_NUM
    )
    building_name: Mapped[Optional[str]] = mapped_column(
        String(50), name=PafColumnNames.BUILDING_NAME
    )
    sub_building_name: Mapped[Optional[str]] = mapped_column(
        String(30), name=PafColumnNames.SUB_BUILDING_NAME
    )
    po_box: Mapped[Optional[str]] = mapped_column(String(6), name=PafColumnNames.PO_BOX)
    department_name: Mapped[Optional[str]] = mapped_column(
        String(60), name=PafColumnNames.DEPARTMENT_NAME
    )
    org_name: Mapped[Optional[str]] = mapped_column(
        String(60), name=PafColumnNames.ORG_NAME
    )
    udprn: Mapped[int] = mapped_column(Integer, name=PafColumnNames.UDPRN)
    postcode_type: Mapped[str] = mapped_column(
        String(1), name=PafColumnNames.POSTCODE_TYPE
    )
    su_org_ind: Mapped[str] = mapped_column(String(1), name=PafColumnNames.SU_ORG_IND)
    delivery_point_suffix: Mapped[str] = mapped_column(
        String(2), name=PafColumnNames.DELIVERY_POINT_SUFFIX
    )
    addr_key: Mapped[int] = mapped_column(Integer, name=PafColumnNames.ADDR_KEY)
    org_key: Mapped[int] = mapped_column(Integer, name=PafColumnNames.ORG_KEY)
    num_households: Mapped[int] = mapped_column(
        Integer, name=PafColumnNames.NUM_HOUSEHOLDS
    )
    locality_key: Mapped[int] = mapped_column(Integer, name=PafColumnNames.LOCALITY_KEY)

    ons_postcode: Mapped[OnsPostcode] = relationship(
        back_populates="addresses", lazy="select"
    )

    def __repr__(self) -> str:
        return self._repr(
            id=self.id,
            postcode=self.postcode,
            post_town=self.post_town,
            dependent_locality=self.dependent_locality,
            thoroughfare_and_desc=self.thoroughfare_and_desc,
            building_num=self.building_num,
            building_name=self.building_name,
            sub_building_name=self.sub_building_name,
            constituency=self.ons_postcode.constituency,
        )
