# UK Constituency Street by Street

This project is basically a collection of scripts to parse CSVs and other files (such as Geopackage) and produces outputs that are more usable for use in election campaigning.

Using open ONS and OS data, and the getAddress.io API to make a queryable database.

No data is stored in this repository, this is simply meant as a tool to combine multiple sources and allow easy processing of regional data. The data sources used:
* [The ONS Statistics Geoportal](https://geoportal.statistics.gov.uk/)
  	* National Statistics Postcode Lookup (NSPL)
  	* Westminster Parliamentary Constituencies
* [OS Open Names](https://osdatahub.os.uk/downloads/open/OpenNames)
* https://getaddress.io/ - see their API for details

## Setting up a development environment

1. Install pipx and then use it to install poetry (see https://python-poetry.org/docs/).
2. Setup a virtual Python environment for developing on this project - use of virtualenvironments is heavily encouraged. I use [pyenv](https://github.com/pyenv/pyenv) + [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) so I can easily have multiple Python versions, but Conda or anything similar is perfect.
	* Poetry supports using pyenv and virtual python environments, see [this guide](https://python-poetry.org/docs/managing-environments/).
3. Run `poetry install --with dev` in your created virtual environment, and you're all set!

## Program usage

After setting everything up using poetry above, you can easily run the program using:

`poetry run ukconstituencystreetbystreet -h`

To initialise config and the database, run:

`poetry run ukconstituencystreetbystreet --init-config`

However, this isn't enough, you need to have downloaded all the files necessary to actually produce useful outputs.

## Finding and downloading the correct information files

### Config setup

`folder_for_data` in `config.ini` sets the folder that contains all datasets for use by this program.

### OS Open Roads

[OS Open Roads](https://www.ordnancesurvey.co.uk/products/os-open-roads) is a dataset of all roads in Great Britain.

`os_open_roads_geopackage` is the config option to specify the filename.

To download:

1. Click 'Get this Product'
2. Click 'Go to the OS DataHub'
3. Download the full GB dataset as `Geopackage` (`.gpkg`) format.

### OS Open Names

[OS Open Names](https://www.ordnancesurvey.co.uk/products/os-open-names) is a dataset of all road names in Great Britain.

`os_openname_csv_folder` is the config option to specify the filename.

To download:

1. Click 'Get this Product'
2. Click 'Go to the OS DataHub'
3. Download the full GB dataset as `CSV` format.

### ONS Geoportal

The [ONS Geoportal](https://geoportal.statistics.gov.uk/) is immensely useful as a resource. We need to download a few resources.

#### West Minister Parliamentry Boundaries (2022)

[This link](https://geoportal.statistics.gov.uk/datasets/d66d6ff58fcc4461970ae003a5cab096_0/explore) may work.

Otherwise, manually click through on the top bar 'Boundaries -> Electoral Boundaries -> Westminister Parliamentry Constituencies' and download the CSV.

`ons_contituencies_csv` is the config option to specify the filename.

#### Local Authority Districts (December 2023) Boundaries UK BFC

[This link](https://geoportal.statistics.gov.uk/datasets/127c4bda06314409a1fa0df505f510e6_0/explore) may work.

Otherwise manually click through on the top bar 'Boundaries -> Administrative Boundaries -> Local Autority Districts' and download the CSV.

`ons_local_auth_csv` is the config option to specify the filename.

#### National Statistics Postcode Lookup - 2021 Census (February 2024)

[This link](https://geoportal.statistics.gov.uk/datasets/e832e833fe5f45e19096800af4ac800c/about) may work, otherwise manually click through on the top bar 'Postcodes -> National Statistics Postcode Lookup (NSPL)' and download the CSV

`ons_postcodes_csv` is the config option to specify the filename.

#### Output Area to Lower layer Super Output Area to Middle layer Super Output Area to Local Authority District (December 2021) Lookup in England and Wales

[This link](https://geoportal.statistics.gov.uk/datasets/b9ca90c10aaa4b8d9791e9859a38ca67_0/explore) may work.

Otherwise manually click through on the top bar 'Lookups -> Census Lookups -> Output Area (2021) Lookups' and download the CSV

`ons_oa_csv` is the config option to specify the filename.

#### MSOA Human Readable Names

[See House of Commons Github](https://houseofcommonslibrary.github.io/msoanames/) and download the CSV. For some reason this isn't on the ONS website, but luckily it is still publicly available.

`ons_msoa_readble_names_csv` is the config option to specify the filename.

#### Census Age by OA

[This link](https://www.nomisweb.co.uk/sources/census_2021_bulk) is the bulk download area for 2021 census data.

To download click the link for 'TS007A' (Age by five-year age bands)

`census_age_by_oa_csv` is the config option to specify the filename.

#### Census Age by MSOA

CURRENTLY UNUSED SINCE CENSUS AGE BY OA HAS MADE THIS REDUNDANT
