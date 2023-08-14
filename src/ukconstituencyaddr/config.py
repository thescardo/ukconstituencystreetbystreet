import configparser
import pathlib
from dataclasses import dataclass

MAIN_STORAGE_FOLDER = pathlib.Path("streetcheck_storage").absolute().resolve()
CONFIG_FILE = MAIN_STORAGE_FOLDER / "config.ini"

config_parser = configparser.ConfigParser()
config: "ConfigClass"


@dataclass
class InputConfig:
    folders_for_csv: pathlib.Path
    royal_mail_paf_csv: pathlib.Path
    ons_constituencies_csv: pathlib.Path
    ons_postcodes_csv: pathlib.Path


@dataclass
class OutputConfig:
    output_folder: pathlib.Path
    use_subfolders: bool


@dataclass
class ConfigClass:
    input: InputConfig
    output: OutputConfig


def parse_config():
    MAIN_STORAGE_FOLDER.mkdir(parents=True, exist_ok=True)

    if CONFIG_FILE.exists():
        if not CONFIG_FILE.is_file():
            raise Exception(
                "Config file for this program has been "
                "replaced with something that isn't a file!"
            )

        config_parser.read(CONFIG_FILE)
    else:
        config_parser["INPUT"] = {
            "folder_for_csvs": "",
            "royal_mail_paf_csv": "CSV PAF.csv",
            "ons_contituencies_csv": "Westminster_Parliamentary_Constituencies_(December_2022)_Names_and_Codes_in_the_United_Kingdom.csv",
            "ons_postcodes_csv": "NSPL21_FEB_2023_UK.csv",
        }

        config_parser["OUTPUT"] = {
            "output_folder": "Streetcheck Output",
            "use_subfolders": "yes",
        }

        with open(CONFIG_FILE, "w") as configfile:
            config_parser.write(configfile)

    input_conf = config_parser["INPUT"]
    output_conf = config_parser["OUTPUT"]

    folder_for_csvs_raw = input_conf["folder_for_csvs"]
    if folder_for_csvs_raw is None or len(folder_for_csvs_raw) == 0:
        folder_for_csvs = pathlib.Path("").resolve()
    else:
        folder_for_csvs = pathlib.Path(folder_for_csvs_raw).resolve()

    global config
    config = ConfigClass(
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
        ),
        output=OutputConfig(
            output_folder=pathlib.Path(output_conf["output_folder"]).resolve(),
            use_subfolders=output_conf.getboolean("use_subfolders"),
        ),
    )


parse_config()
