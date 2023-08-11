""""""

import argparse
import pathlib

ROYAL_MAIL_CSV = pathlib.Path("/home/the/Dev/GNDR/Postcode lookup/RoyalMail Data/CSV PAF/CSV PAF.csv")
ONS_POSTCODE_CSV = pathlib.Path("/home/the/Dev/GNDR/Postcode lookup/ONS Data/Data/NSPL21_FEB_2023_UK.csv")
ONS_CONSTITUENCY_CSV = pathlib.Path("/home/the/Dev/GNDR/Postcode lookup/ONS Data/Westminster_Parliamentary_Constituencies_(December_2022)_Names_and_Codes_in_the_United_Kingdom.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="UKConstituencyStreetCheck",
        description="Processes ONS data along with Royal Mail data "
        "to produce lists of addresses in a constituency",
        epilog="You need to download the data yourself, see the README",
    )

    parser.add_argument("-g", "--graphicmode", action="store_true")  # on/off flag
