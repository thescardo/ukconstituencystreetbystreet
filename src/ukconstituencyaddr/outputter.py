""""""
import argparse
import concurrent.futures
import json
import logging
import multiprocessing
from typing import List, Optional
import difflib
import pathlib

import pandas as pd
import tqdm
from sqlalchemy.orm import Session

from ukconstituencyaddr import (
    address_fetcher,
    config,
    ons_constituencies,
    ons_postcodes,
    os_opennames,
    ons_msoa,
    ons_local_authority_district,
)
from ukconstituencyaddr.db import db_repr_sqlite as db_repr


class ConstituencyInfoOutputter:
    """Class that outputs csvs of addresses based on a constituency or other desired attribute"""

    def __init__(self) -> None:
        self.constituency_parser = ons_constituencies.ConstituencyCsvParser()
        self.postcode_parser = ons_postcodes.PostcodeCsvParser()
        self.osopennames_parser = os_opennames.OsOpenNamesCsvsParser()
        self.local_authority_parser = (
            ons_local_authority_district.LocalAuthorityCsvParser()
        )
        self.msoa_parser = ons_msoa.OnsMsoaCsvParser()
        self.census_age_by_msoa_parser = ons_msoa.CensusAgeByMsoaCsvParser()

        self.street_fetcher = address_fetcher.AddrFetcher()

        self.output_folder = config.config.output.output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.use_subfolders = config.config.output.use_subfolders

        self.engine = db_repr.get_engine()

        self.logger = logging.getLogger(self.__class__.__name__)

    def _abs_get_folder(self, category_name: str, name: str) -> pathlib.Path:
        """Returns the Path of the given category folder"""
        if self.use_subfolders:
            out = self.output_folder / category_name / name
            out.mkdir(parents=True, exist_ok=True)
        else:
            out = self.output_folder / category_name

        return out

    def get_specific_constituency_folder(self, constituency_name: str) -> pathlib.Path:
        """Returns the Path of the given constituency"""
        return self._abs_get_folder("Westminster Constituency", constituency_name)

    def get_constituency_folder(self) -> pathlib.Path:
        """Returns the Path of the given constituency"""
        out = self.output_folder / "Westminster Constituency"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def get_specific_local_authority_folder(
        self, constituency_name: str
    ) -> pathlib.Path:
        """Returns the Path of the local authority"""
        return self._abs_get_folder("Local Authority", constituency_name)

    def get_local_authority_folder(self) -> pathlib.Path:
        """Returns the Path of the given local authority"""
        out = self.output_folder / "Local Authority"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def process_csvs(self):
        """
        Parses all CSVs that are required to map constituencies
        and other information onto a given address
        """
        parsers = [
            self.constituency_parser,
            self.postcode_parser,
            self.osopennames_parser,
            self.local_authority_parser,
            self.msoa_parser,
            self.census_age_by_msoa_parser,
        ]
        process = tqdm.tqdm(total=len(parsers), desc="Importing CSVs to local database")
        for x in parsers:
            try:
                x.process_csv()
            except:
                self.logger.error("Caught exception, clearing constituency cache")
                x.clear_all()
                raise
            process.update(1)

    def fetch_addresses_in_constituencies(self, constituency_names: List[str]):
        """Downloads all address data for the given constituency names"""
        self.street_fetcher.fetch_constituencies(constituency_names)

    def fetch_addresses_in_local_authorities(self, constituency_names: List[str]):
        """Downloads all address data for the given local_authority names"""
        self.street_fetcher.fetch_local_authorities(constituency_names)

    def make_csv_streets_in_constituency(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Make CSV of all streets in a given constituency"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            if name is None:
                name = self.constituency_parser.get_constituency(
                    id
                ).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != None)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if id is not None:
                mid_query = base_query.filter(
                    db_repr.OnsConstituency.oid == id
                )
            else:
                mid_query = base_query.filter(
                    db_repr.OnsConstituency.name == name
                )

            final_query = mid_query.distinct(
                db_repr.SimpleAddress.thoroughfare_or_desc
            ).with_entities(db_repr.SimpleAddress.thoroughfare_or_desc)

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(
                    f"Found no addresses for constituency {name}"
                )
            else:
                dir = self.get_specific_constituency_folder(name)
                df.to_csv(str(dir / f"{name} Street Names.csv"))

    def make_csv_addresses_in_constituency(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Make CSV of all addresses in a given constituency"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            if name is None:
                name = self.constituency_parser.get_constituency(
                    id
                ).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != None)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if id is not None:
                final_query = base_query.filter(
                    db_repr.OnsConstituency.oid == id
                )
            else:
                final_query = base_query.filter(
                    db_repr.OnsConstituency.name == name
                )

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(
                    f"Found no addresses for constituency {name}"
                )
            else:
                dir = self.get_specific_constituency_folder(name)
                df.to_csv(str(dir / f"{name} Addresses.csv"))

    def make_csvs_for_all_constituencies(self):
        """
        For all constituencies in the database, make a CSV of
        addresses in each constituency
        """
        with Session(self.engine) as session:
            all_constituencies = [
                constituency.oid
                for constituency in session.query(db_repr.OnsConstituency).all()
            ]

            def make_csvs_for_constituency(constituency_id: str) -> bool:
                self.make_csv_streets_in_constituency(id=constituency_id)
                self.make_csv_addresses_in_constituency(id=constituency_id)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=multiprocessing.cpu_count()
            ) as executor:
                results = list(
                    tqdm.tqdm(
                        executor.map(make_csvs_for_constituency, all_constituencies),
                        total=len(all_constituencies),
                        desc="Outputting addresses to CSV",
                    )
                )
            return results

    def get_similar_constituencies(self, search_term: str) -> List[str]:
        """Returns constituencies that match the name of the search term"""
        with Session(self.engine) as session:
            constituencies = list(session.query(db_repr.OnsConstituency).all())
            constituency_names = [constituency.name for constituency in constituencies]

            return difflib.get_close_matches(
                search_term, constituency_names, n=5, cutoff=0.3
            )

    def percent_fetched_for_constituency(self, name: str):
        """
        Prints the percentage of postcode areas that address data
        has been fetched for in a given constituency
        """
        with Session(self.engine) as session:
            num_postcodes_in_area = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == name)
                .count()
            )
            num_postcodes_fetched = (
                session.query(db_repr.PostcodeFetched)
                .join(db_repr.OnsConstituency)
                .where(db_repr.OnsConstituency.name == name)
                .count()
            )
            percent = (num_postcodes_fetched / num_postcodes_in_area) * 100

            print_str = f"In {name}, {num_postcodes_in_area=}, {num_postcodes_fetched=}, {percent=}"
            self.logger.info(
                f"In {name}, {num_postcodes_in_area=}, {num_postcodes_fetched=}, {percent=}"
            )
            print(print_str)

    def percent_fetched_for_local_authority(self, name: str):
        """
        Prints the percentage of postcode areas that address data
        has been fetched for in a given local authority
        """
        with Session(self.engine) as session:
            num_postcodes_in_area = (
                session.query(db_repr.OnsPostcode)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.OnsLocalAuthorityDistrict.name == name)
                .count()
            )
            num_postcodes_fetched = (
                session.query(db_repr.PostcodeFetched)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.OnsLocalAuthorityDistrict.name == name)
                .count()
            )
            percent = (num_postcodes_fetched / num_postcodes_in_area) * 100

            print_str = f"In {name}, {num_postcodes_in_area=}, {num_postcodes_fetched=}, {percent=}"
            self.logger.info(
                f"In {name}, {num_postcodes_in_area=}, {num_postcodes_fetched=}, {percent=}"
            )
            print(print_str)

    def make_csv_streets_in_local_authority(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Make CSV of all streets in a given local authority"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            if name is None:
                name = self.local_authority_parser.get_local_authority(id).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != None)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if id is not None:
                mid_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.oid == id
                )
            else:
                mid_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.name == name
                )

            final_query = mid_query.distinct(
                db_repr.SimpleAddress.thoroughfare_or_desc
            ).with_entities(db_repr.SimpleAddress.thoroughfare_or_desc)

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(f"Found no addresses for local authority {name}")
            else:
                dir = self.get_specific_local_authority_folder(name)
                df.to_csv(str(dir / f"{name} Street Names.csv"))

    def make_csv_addresses_in_local_authority(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Make CSV of all addresses in a given local authority"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            if name is None:
                name = self.local_authority_parser.get_local_authority(id).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != None)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if id is not None:
                final_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.oid == id
                )
            else:
                final_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.name == name
                )

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(f"Found no addresses for local_authority {name}")
            else:
                dir = self.get_specific_local_authority_folder(name)
                df.to_csv(str(dir / f"{name} Addresses.csv"))

    def make_csv_postcodes_by_age_local_authority(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ):
        """Make CSV of all addresses in a given local authority"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            if name is None:
                name = self.local_authority_parser.get_local_authority(id).name

            base_query = (
                session.query(db_repr.SimpleAddress)
                .join(db_repr.OnsPostcode)
                .join(db_repr.OnsLocalAuthorityDistrict)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != None)
                .where(db_repr.SimpleAddress.thoroughfare_or_desc != "")
            )

            if id is not None:
                final_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.oid == id
                )
            else:
                final_query = base_query.filter(
                    db_repr.OnsLocalAuthorityDistrict.name == name
                )

            df = pd.read_sql(final_query.selectable, self.engine)
            if len(df.index) == 0:
                self.logger.debug(f"Found no addresses for local_authority {name}")
            else:
                dir = self.get_specific_local_authority_folder(name)
                df.to_csv(str(dir / f"{name} Addresses.csv"))

    def make_csv_postcodes_ranked_by_age_in_constituencies(
        self,
        names: List[str],
    ):
        """Make CSV of all postcodes in a westminister constituencies with the % of young people in that postcode"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            constituencies: List[db_repr.OnsConstituency] = []
            for name in names:
                result = self.constituency_parser.get_constituency_by_name(name)
                if result is None:
                    raise Exception(f"Failed to find authority named {name}")
                constituencies.append(result)

            # Query for all required authorities
            postcode_dfs = []
            for constituency in constituencies:
                query = (
                    session.query(
                        db_repr.OnsPostcode.postcode,
                        db_repr.OnsConstituency.name,
                        db_repr.OnsLocalAuthorityDistrict.name,
                        db_repr.CensusAgeByMsoa.observed_count,
                        db_repr.CensusAgeByMsoa.percent_of_msoa,
                    )
                    .filter(
                        db_repr.OnsPostcode.local_authority_district_id
                        == db_repr.OnsLocalAuthorityDistrict.oid
                    )
                    .filter(
                        db_repr.OnsPostcode.msoa_id == db_repr.CensusAgeByMsoa.msoa_id
                    )
                    .filter(
                        db_repr.CensusAgeByMsoa.age_range
                        == db_repr.CensusAgeRange.R_16_35
                    )
                    .filter(db_repr.OnsConstituency.oid == constituency.oid)
                    .filter(
                        db_repr.OnsLocalAuthorityDistrict.oid
                        == db_repr.OnsPostcode.local_authority_district_id
                    )
                )

                df = pd.read_sql(query.selectable, self.engine)
                postcode_dfs.append(df)

            combined_df = pd.concat(postcode_dfs, ignore_index=True, sort=False)
            combined_df = combined_df.sort_values(
                [
                    "census_age_by_msoa_percent_of_msoa",
                    "census_age_by_msoa_observed_count",
                    "ons_postcode_postcode",
                ],
                ascending=[False, False, True],
            )
            combined_df = combined_df.round({"census_age_by_msoa_percent_of_msoa": 2})
            combined_df = combined_df.rename(
                columns={
                    "ons_postcode_postcode": "Postcode",
                    "ons_constituency_name": "Constituency Name",
                    "ons_local_auth_district_name": "Local Authority Name",
                    "census_age_by_msoa_observed_count": "Count of People",
                    "census_age_by_msoa_percent_of_msoa": "Percent of People",
                }
            )

            if len(combined_df.index) == 0:
                self.logger.debug(f"Found no postcodes for local authorities {names}")
            else:
                dir = self.get_local_authority_folder()
                combined_df.to_csv(
                    str(
                        dir
                        / f"{'_'.join(names)} Postcodes by percentage {db_repr.CensusAgeRange.R_16_35}.csv"
                    )
                )

    def make_csv_postcodes_ranked_by_age_in_local_authorities(
        self,
        names: List[str],
    ):
        """Make CSV of all postcodes in a local authority with the % of young people in that postcode"""
        assert id is not None or name is not None
        with Session(self.engine) as session:
            authorities: List[db_repr.OnsLocalAuthorityDistrict] = []
            for name in names:
                result = self.local_authority_parser.get_local_authority_by_name(name)
                if result is None:
                    raise Exception(f"Failed to find authority named {name}")
                authorities.append(result)

            # Query for all required authorities
            postcode_dfs = []
            for authority in authorities:
                query = (
                    session.query(
                        db_repr.OnsPostcode.postcode,
                        db_repr.OnsConstituency.name,
                        db_repr.OnsLocalAuthorityDistrict.name,
                        db_repr.CensusAgeByMsoa.observed_count,
                        db_repr.CensusAgeByMsoa.percent_of_msoa,
                    )
                    .filter(
                        db_repr.OnsPostcode.local_authority_district_id
                        == db_repr.OnsLocalAuthorityDistrict.oid
                    )
                    .filter(
                        db_repr.OnsPostcode.msoa_id == db_repr.CensusAgeByMsoa.msoa_id
                    )
                    .filter(
                        db_repr.CensusAgeByMsoa.age_range
                        == db_repr.CensusAgeRange.R_16_35
                    )
                    .filter(
                        db_repr.OnsConstituency.oid
                        == db_repr.OnsPostcode.constituency_id
                    )
                    .filter(db_repr.OnsLocalAuthorityDistrict.oid == authority.oid)
                )

                df = pd.read_sql(query.selectable, self.engine)
                postcode_dfs.append(df)

            combined_df = pd.concat(postcode_dfs, ignore_index=True, sort=False)
            combined_df = combined_df.sort_values(
                [
                    "census_age_by_msoa_percent_of_msoa",
                    "census_age_by_msoa_observed_count",
                    "ons_postcode_postcode",
                ],
                ascending=[False, False, True],
            )
            combined_df = combined_df.round({"census_age_by_msoa_percent_of_msoa": 2})
            combined_df = combined_df.rename(
                columns={
                    "ons_postcode_postcode": "Postcode",
                    "ons_constituency_name": "Constituency Name",
                    "ons_local_auth_district_name": "Local Authority Name",
                    "census_age_by_msoa_observed_count": "Count of People",
                    "census_age_by_msoa_percent_of_msoa": "Percent of People",
                }
            )

            if len(combined_df.index) == 0:
                self.logger.debug(f"Found no postcodes for local authorities {names}")
            else:
                dir = self.get_local_authority_folder()
                combined_df.to_csv(
                    str(
                        dir
                        / f"{'_'.join(names)} Postcodes by percentage {db_repr.CensusAgeRange.R_16_35}.csv"
                    )
                )


def output_csvs():
    parser = argparse.ArgumentParser(
        prog="UKConstituencyStreetCheck",
        description="Processes ONS data along with Royal Mail data "
        "to produce lists of addresses in a constituency."
        "When run the first time, it will create a folder where it will"
        "keep all of its config and local cache, and another folder"
        "(configurable) to output all CSVs that you wish to create.",
        epilog="You need to download the data yourself, see the README",
    )
    parser.add_argument(
        "-i",
        "--init_config",
        action="store_true",
        help="If specified, only initialise config and config folder",
    )
    parser.add_argument(
        "-b",
        "--build_cache",
        action="store_true",
        help="If specified, build the local database cache",
    )
    parser.add_argument(
        "-f",
        "--fetch",
        action="store_true",
        help="If specified, fetches address data from getaddress.io",
    )
    parser.add_argument(
        "-a",
        "--postcodes_by_age",
        action="store_true",
        help="If specified, output a CSV ranked by percentage of people 16-35 in that location",
    )
    parser.add_argument(
        "-l",
        "--local_authority",
        action="store_true",
        help="If specified, use local authorities",
    )
    parser.add_argument(
        "-c",
        "--constituency",
        action="store_true",
        help="If specified, use constituencies",
    )
    parser.add_argument(
        "-n",
        "--num_scraped",
        action="store_true",
        help="If specified, return percentage of fetched "
        "postcodes from constituency argument",
    )
    parser.add_argument(
        "-q",
        "--addressio_limits",
        action="store_true",
        help="Return usage limits for address.io",
    )
    parser.add_argument(
        "-j",
        "--cleanup_addresses",
        action="store_true",
        help="Do a cleanup of all stored addresses",
    )
    parser.add_argument(
        "-s",
        "--output_csvs",
        action="store_true",
        help="Output csvs",
    )
    parser.add_argument(
        "-d",
        "--debug_get_address",
        action="store_true",
        help="Return usage limits for address.io",
    )

    args = parser.parse_args()

    if not args.init_config:
        config.init_loggers()
        config.parse_config()

        comb = ConstituencyInfoOutputter()

        data_opts = config.config.data_opts

        if args.debug_get_address:
            print(
                json.dumps(
                    address_fetcher.get_address_resp_for_postcode(
                        args.debug_get_address, full_lookup=False
                    )[1],
                    indent=4,
                )
            )
            return
        
        if args.cleanup_addresses:
            comb.street_fetcher.cleanup_all_addresses()
            return

        if args.addressio_limits:
            print(comb.street_fetcher.num_req_manger.get_limits())
            return

        if args.num_scraped:
            if args.constituency:
                for constituency in data_opts.constituencies:
                    comb.percent_fetched_for_constituency(data_opts.constituencies)
            else:
                for local_authority in data_opts.local_authorities:
                    comb.percent_fetched_for_local_authority(data_opts.constituencies)
            return

        if args.fetch:
            if args.constituency:
                comb.fetch_addresses_in_constituencies(data_opts.constituencies)
            elif args.local_authority:
                comb.fetch_addresses_in_local_authorities(
                    data_opts.local_authorities
                )
            return

        if args.build_cache:
            comb.process_csvs()
            return

        if args.output_csvs:
            if args.constituency:
                for constituency in data_opts.constituencies:
                    comb.make_csv_streets_in_constituency(name=constituency)
                    comb.make_csv_addresses_in_constituency(
                        name=constituency
                    )
            elif args.local_authority:
                for local_authority in data_opts.local_authorities:
                    comb.make_csv_streets_in_local_authority(name=local_authority)
                    comb.make_csv_addresses_in_local_authority(
                        name=local_authority
                    )
            return

        if args.postcodes_by_age:
            if args.constituency:
                comb.make_csv_postcodes_ranked_by_age_in_constituencies(
                    str(data_opts.constituencies).split(",")
                )
            elif args.local_authority:
                comb.make_csv_postcodes_ranked_by_age_in_local_authorities(
                    data_opts.local_authorities
                )
            return
