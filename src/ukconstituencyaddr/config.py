"""
Module to allow configuration of this program
"""

import configparser
import logging
import pathlib
from dataclasses import dataclass
from typing import List

# Create a default folder
MAIN_STORAGE_FOLDER = pathlib.Path("streetcheck_storage").absolute().resolve()
CONFIG_FILE = MAIN_STORAGE_FOLDER / "config.ini"

config_parser = configparser.ConfigParser()
conf: "RootConfigClass"


def init_loggers():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(filename)s:"
        "%(lineno)d %(name)s %(message)s",
        handlers=[
            logging.FileHandler(MAIN_STORAGE_FOLDER / "streetcheck.log"),
        ],
    )
    logging.getLogger()

    for name in ["sqlalchemy", "sqlalchemy.engine"]:
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)


@dataclass
class DataOptsConfig:
    """Data manipulation configuration"""

    constituencies: List[str]
    local_authorities: List[str]


@dataclass
class InputConfig:
    """Input file locations configuration"""

    folders_for_csv: pathlib.Path
    royal_mail_paf_csv: pathlib.Path
    ons_constituencies_csv: pathlib.Path
    ons_postcodes_csv: pathlib.Path
    os_openname_csv_folder: pathlib.Path
    ons_local_auth_csv: pathlib.Path
    ons_oa_csv: pathlib.Path
    ons_msoa_csv: pathlib.Path
    census_age_by_msoa_csv: pathlib.Path
    census_age_by_oa_csv: pathlib.Path


@dataclass
class OutputConfig:
    """Output folder locations configuration"""

    output_folder: pathlib.Path
    use_subfolders: bool


@dataclass
class AddressDownloadConfig:
    """Config to download address data from getaddress.io"""

    allow_getting_full_address: bool
    max_requests_per_5_mins: int
    get_address_io_api_key: str
    get_address_io_admin_key: str


@dataclass
class RootConfigClass:
    """Root container for all config for easy of access to rest of the program"""

    data_opts: DataOptsConfig
    input: InputConfig
    output: OutputConfig
    scraping: AddressDownloadConfig


def parse_config():
    """Reads config from file and puts it into ConfigClass"""
    MAIN_STORAGE_FOLDER.mkdir(parents=True, exist_ok=True)

    if CONFIG_FILE.exists():
        # Read config if it exists and is a file
        if not CONFIG_FILE.is_file():
            raise Exception(
                "Config file for this program has been "
                "replaced with something that isn't a file!"
            )

        config_parser.read(CONFIG_FILE)
    else:
        # Otherwise fill the config with defaults and write it to the default
        # config location
        config_parser["INPUT"] = {
            "folder_for_csvs": "",
            "royal_mail_paf_csv": "CSV PAF.csv",
            "ons_contituencies_csv": "Westminster_Parliamentary_Constituencies_"
            "(December_2022)_Names_and_Codes"
            "_in_the_United_Kingdom.csv",
            "ons_postcodes_csv": "NSPL21_FEB_2023_UK.csv",
            "os_openname_csv_folder": "os_openname_csv_folder",
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

        with open(CONFIG_FILE, "w") as configfile:
            config_parser.write(configfile)

    input_conf = config_parser["INPUT"]
    output_conf = config_parser["OUTPUT"]
    scraping_conf = config_parser["SCRAPING"]
    data_opts = config_parser["DATA_OPTS"]

    # Convert if necessary to pathlip.Path
    folder_for_csvs_raw = input_conf["folder_for_csvs"]
    if folder_for_csvs_raw is None or len(folder_for_csvs_raw) == 0:
        folder_for_csvs = pathlib.Path("").resolve()
    else:
        folder_for_csvs = pathlib.Path(folder_for_csvs_raw).resolve()

    # Read all config and convert it to correct types for easy of use in the
    # rest of the program
    global conf
    conf = RootConfigClass(
        input=InputConfig(
            folders_for_csv=folder_for_csvs,
            royal_mail_paf_csv=(
                folder_for_csvs / input_conf["royal_mail_paf_csv"]
            ).resolve(),
            ons_constituencies_csv=(
                folder_for_csvs / input_conf["ons_contituencies_csv"]
            ).resolve(),
            ons_postcodes_csv=(
                folder_for_csvs / input_conf["ons_postcodes_csv"]
            ).resolve(),
            os_openname_csv_folder=pathlib.Path(input_conf["os_openname_csv_folder"]),
            ons_local_auth_csv=(
                folder_for_csvs / input_conf["ons_local_auth_csv"]
            ).resolve(),
            ons_oa_csv=(folder_for_csvs / input_conf["ons_oa_csv"]).resolve(),
            ons_msoa_csv=(folder_for_csvs / input_conf["ons_msoa_csv"]).resolve(),
            census_age_by_msoa_csv=(
                folder_for_csvs / input_conf["census_age_by_msoa_csv"]
            ).resolve(),
            census_age_by_oa_csv=(
                folder_for_csvs / input_conf["census_age_by_oa_csv"]
            ).resolve(),
        ),
        output=OutputConfig(
            output_folder=pathlib.Path(output_conf["output_folder"]).resolve(),
            use_subfolders=output_conf.getboolean("use_subfolders"),
        ),
        scraping=AddressDownloadConfig(
            allow_getting_full_address=scraping_conf.getboolean(
                "allow_getting_full_address"
            ),
            get_address_io_api_key=scraping_conf["get_address_io_api_key"],
            get_address_io_admin_key=scraping_conf["get_address_io_admin_key"],
            max_requests_per_5_mins=int(scraping_conf["max_requests_per_5_mins"]),
        ),
        data_opts=DataOptsConfig(
            constituencies=str(data_opts["constituencies"]).split("|"),
            local_authorities=str(data_opts["local_authorities"]).split("|"),
        ),
    )
