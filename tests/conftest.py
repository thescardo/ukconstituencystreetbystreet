import configparser
import os
import pathlib
import shutil
import pytest

from ukconstituencystreetbystreet import config
from ukconstituencystreetbystreet.db import cacher
from ukconstituencystreetbystreet.db.db_repr_sqlite import (
    Base,
    get_engine,
)

dir_path = pathlib.Path(__file__).parent
TEST_STORAGE_FOLDER = (
    (pathlib.Path(dir_path) / "streetcheck_test_storage").absolute().resolve()
)
TEST_CSV_FOLDER = (
    (pathlib.Path(dir_path) / "streetcheck_test_csvs").absolute().resolve()
)

TEST_STORAGE_FOLDER.mkdir(parents=True, exist_ok=True)
TEST_CSV_FOLDER.mkdir(parents=True, exist_ok=True)

TEST_CACHE_DB_FILE = TEST_STORAGE_FOLDER / "test.sqlite"


@pytest.fixture(autouse=True, scope="session")
def setup_config():
    config.MAIN_STORAGE_FOLDER = TEST_STORAGE_FOLDER
    folder_for_data = TEST_CSV_FOLDER

    config_parser = configparser.ConfigParser()
    config_parser["INPUT"] = {
        "folder_for_data": "",
        "ons_contituencies_csv": "Westminster_Parliamentary_Constituencies_"
        "(December_2022)_Names_and_Codes"
        "_in_the_United_Kingdom.csv",
        "os_openname_csv_folder": "os_openname_csv_folder",
        "ons_postcodes_csv": "NSPL21_FEB_2023_UK.csv",
        "ons_local_auth_csv": "Local_Authority_Districts_December_2023_Boundaries_UK_BFE_6619220630419597412.csv",
        "ons_oa_csv": "Output_Areas_2021_EW_BFE_V9_-4867123113532843655.csv",
        "ons_msoa_csv": "MSOA_2021_EW_BFE_V7_4158844050038459526.csv",
        "census_age_by_msoa_csv": "Census Age Data by MSOA.csv",
        "census_age_by_oa_csv": "census2021-ts007a-oa.csv",
    }

    config_parser["OUTPUT"] = {
        "output_folder": "Streetcheck Output",
        "use_subfolders": "yes",
    }

    config_parser["SCRAPING"] = {
        "allow_getting_full_address": "no",
        "get_address_io_api_key": "",
        "get_address_io_admin_key": "",
        "max_requests_per_5_mins": 2000,
    }

    config_parser["DATA_OPTS"] = {"constituencies": "", "local_authorities": ""}

    input_conf = config_parser["INPUT"]
    output_conf = config_parser["OUTPUT"]
    scraping_conf = config_parser["SCRAPING"]
    data_opts = config_parser["DATA_OPTS"]

    config.conf = config.RootConfigClass(
        input=config.InputConfig(
            folder_for_data=folder_for_data,
            ons_constituencies_csv=(
                folder_for_data / input_conf["ons_contituencies_csv"]
            ).resolve(),
            ons_postcodes_csv=(
                folder_for_data / input_conf["ons_postcodes_csv"]
            ).resolve(),
            os_openname_csv_folder=pathlib.Path(input_conf["os_openname_csv_folder"]),
            ons_local_auth_csv=(
                folder_for_data / input_conf["ons_local_auth_csv"]
            ).resolve(),
            ons_oa_csv=(folder_for_data / input_conf["ons_oa_csv"]).resolve(),
            ons_msoa_csv=(folder_for_data / input_conf["ons_msoa_csv"]).resolve(),
            census_age_by_msoa_csv=(
                folder_for_data / input_conf["census_age_by_msoa_csv"]
            ).resolve(),
            census_age_by_oa_csv=(
                folder_for_data / input_conf["census_age_by_oa_csv"]
            ).resolve(),
        ),
        output=config.OutputConfig(
            output_folder=pathlib.Path(output_conf["output_folder"]).resolve(),
            use_subfolders=output_conf.getboolean("use_subfolders"),
        ),
        scraping=config.AddressDownloadConfig(
            allow_getting_full_address=scraping_conf.getboolean(
                "allow_getting_full_address"
            ),
            get_address_io_api_key=scraping_conf["get_address_io_api_key"],
            get_address_io_admin_key=scraping_conf["get_address_io_admin_key"],
            max_requests_per_5_mins=scraping_conf["max_requests_per_5_mins"],
        ),
        data_opts=config.DataOptsConfig(
            constituencies=str(data_opts["constituencies"]).split(","),
            local_authorities=str(data_opts["local_authorities"]).split(","),
        ),
    )

    config.init_loggers()


def get_test_engine():
    return get_engine(TEST_CACHE_DB_FILE)


@pytest.fixture(autouse=True, scope="session")
def setup_db():
    cacher.create_all(get_test_engine())


@pytest.fixture(autouse=True, scope="session")
def setup_folders():
    # Clean up before starting
    shutil.rmtree(str(TEST_STORAGE_FOLDER))

    TEST_STORAGE_FOLDER.mkdir(parents=True, exist_ok=True)

    yield
