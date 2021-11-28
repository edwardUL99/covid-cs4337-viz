"""
This module retrieves the COVID-19 data from the github repository (already pulled to a specified local repo),
and persists it in data.csv. It is intended to be ran as a main script to be used separately to main.py
"""
import csv
import argparse

import pandas as pd
import datetime
import os

basedir = os.path.abspath(os.path.dirname(__file__))

import pandasutils as pu
from fields import *

# TODO maybe check if df already exists and then only append data which has a daterecorded >= the max currently recorded

# The filename for storing the retrieved data
DATA = os.path.join(basedir, 'data')
DATA_FILE = os.path.join(DATA, 'data.csv')
VACCINATIONS_URL = 'https://www.kaggle.com/gpreda/covid-world-vaccination-progress'
VACCINATIONS_FILE = os.path.join(DATA, 'country_vaccinations.csv')
GITHUB_DESTINATION_DEFAULT = os.path.join(DATA, 'covid19data')
GITHUB_URL = 'git@github.com:CSSEGISandData/COVID-19.git'
# The filename storing the last date retrieved
LAST_DATE = 'last-date.txt'
# The first data of COVID-19 data stored
FIRST_DATE = '01-22-2020'
# The format for parsing dates
DATE_FORMAT = '%m-%d-%Y'
# The fields of the downloaded data we want to keep
FIELDS_TO_KEEP = ALL_FIELDS

# Additional preprocessors to add while loading data
ADDITIONAL_PROCESSORS = []

parser = argparse.ArgumentParser(description='Pulls and cleans CSSE GIS Covid-19 data and vaccination data from'
                                             f' {VACCINATIONS_URL} into a '
                                             'single DataFrame. By default, it pulls data into the local directory '
                                             'under the data directory')

group = parser.add_mutually_exclusive_group()
group.add_argument('-l', '--local',
                   help='If the git repository has already been cloned, provide the path here. The data'
                        ' will be used as is and not pulled to update it',
                   required=False, default=None)
group.add_argument('-g', '--github', help='Specifies the destination path for the git repo. If the git repo does not'
                                          ' exist, it will be cloned. If it does, it will be pulled to update the new'
                                          ' data. For this to work, you need to have a GitHub ssh key setup. See '
                                          'https://docs.github.com/en/authentication/connecting-to-github-with-ssh',
                   required=False, default=GITHUB_DESTINATION_DEFAULT)
parser.add_argument('-o', '--output', help='The path to the output file', required=False, default=DATA_FILE)

args = parser.parse_args()
_output_messages=False


def _log(msg):
    if _output_messages:
        print(msg)


def _do_github(github):
    """
    Clone/pull from github
    :param github: the path to the github destination
    :return: None
    """
    from git import Repo

    if not os.path.isdir(github):
        _log(f'Cloning GitHub repository to {github}')
        repo = Repo.clone_from(url=GITHUB_URL, to_path=github)
        repo.git.checkout('master')
    else:
        _log(f'Pulling updates for GitHub repository in {github}')
        repo = Repo(github)
        repo.git.checkout('master')
        repo.remotes.origin.pull()


def _pull_data(local=None, github=None, output=None):
    """
    Pull the data from the appropriate datasource. If local and it exists, nothing is done. If it doesn't exist, an
    error is thrown. If github and the repo doesn't exist, it will be cloned. If it does exist, it will be pulled.
    :param local: the path to the local repo if already cloned
    :param github: the path to the repo to clone/pull from github
    :param output: the output path for the data file
    :return: None
    """
    if local is None and github is None:
        raise RuntimeError('You need to specify either a local repository or a destination to clone the GitHub '
                           'repository')
    elif local == github:
        raise RuntimeError('You can only specify one of: local repository or GitHub repository')

    if output is None:
        output = DATA
    else:
        output = os.path.dirname(output)

    if not os.path.isdir(DATA):
        os.makedirs(DATA)

    if not os.path.isdir(output):
        os.makedirs(output)

    if local:
        if not os.path.isdir(local):
            raise RuntimeError(f'{local} is either not a directory or it does not exist')
        else:
            _log(f'Using local repository {local}')
    elif github:
        _do_github(github)

    if not os.path.isfile(VACCINATIONS_FILE):
        print(f'Vaccinations data file {VACCINATIONS_FILE} does not exist.\nUnfortunately, it cannot be automatically '
              f'downloaded.\nPlease download it from the following url: '
              f'{VACCINATIONS_URL}.\nSign in > Click Download > Extract the Zip > Copy country_vaccinations.csv '
              f'to {DATA}')

        exit(1)


def _date_to_string(date):
    """
    Converts the date to string
    :param date: the date to convert
    :return: the converted string
    """
    return date.strftime(DATE_FORMAT)


class DataRetriever:
    """
    This class retrieves data from some source for a particular date
    """

    def retrieve(self, date):
        """
        Retrieve the data frame for this particular day
        :param date: the date to retrieve the dataframe for
        :return: the daily dataframe
        """
        raise NotImplementedError


class LocalDataRetriever(DataRetriever):
    """
    This class retrieves data locally if the github repo has been cloned
    """

    def __init__(self, repo_root_path):
        """
        Create a LocalDataRetriever
        :param repo_root_path: the path to the root of the locally cloned repo
        """
        self._root = repo_root_path

        if not os.path.isdir(self._root):
            raise RuntimeError(f'{self._root} does not exist')

        self._data = os.path.join(self._root, 'csse_covid_19_data', 'csse_covid_19_daily_reports')

    def _create_path(self, date):
        """
        Create the path for the given date
        :param date: the date of the csv file to read
        :return: the path
        """
        return os.path.join(self._data, f'{_date_to_string(date)}.csv')

    def retrieve(self, date):
        """
        Retrieve the data frame for this particular day
        :param date: the date to retrieve the dataframe for
        :return: the daily dataframe
        """
        path = self._create_path(date)

        if not os.path.isfile(path):
            return None
        else:
            return pu.from_csv(path)


def _get_data_retriever(local, github) -> DataRetriever:
    """
    Set up the DataRetriever for use to read in the csse covid 19 data based on either local repo or github repo
    :param local: the path to the local repo if already cloned
    :param github: the path to the repo to clone/pull from github
    :return: the data retriever to retrieve the data
    """
    path = local if local else github

    return LocalDataRetriever(path)


def _get_dates():
    """
    This is a generator function that yields dates from last_retrieved_date (inclusive) adding one day to the date until
    it reaches the current date
    :return: None, but it yields dates from last retrieved date to current date, incremented by 1 day
    """
    start_date = datetime.datetime.strptime(FIRST_DATE, DATE_FORMAT).date()
    end_date = datetime.datetime.now().date()

    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date = current_date + datetime.timedelta(days=1)


def _preprocess_df(df: pu.DataFrame, date):
    """
    Preprocesses the loaded dataframe, keeping only fields we're interested in
    :param df: the dataframe to preprocess
    :param date: the date this of this day
    :return: the preprocessed dataframe
    """
    # Some of the data files had Country_Region instead of Country/Region, so if that is so, rename it to Country/Region
    preprocessed_df = df.fill_required(FIELDS_TO_KEEP, rename_mapper={'Country_Region': COUNTRY_REGION})
    preprocessed_df[DATE_RECORDED] = date

    return preprocessed_df


def _load_day_data(date, data_retriever: DataRetriever):
    """
    Reads the day data for the provided date and returns the dataframe and if no error occurs
    :param date: the date to read the data for
    :return: the dataframe or none if an error occurred
    """
    df = data_retriever.retrieve(date)

    return _preprocess_df(df, date) if df is not None else None


def _preprocess_whole_df(df: pu.DataFrame):
    """
    Preprocess the final produced dataframe with all daily totals
    :param df: the dataframe to preprocess
    :return: the preprocessed data frame
    """

    def handle_country_daily_cases(df, country):
        """
        Calculates the daily numbers from the given totals for the specified country
        :param df: the dataframe to perform the calculations on
        :param country: the country to filter the dataframe with
        :return: the processed dataframe
        """

        def subtract(df, field, new_field=None):
            """
            With the given dataframe and field, this method takes value at field row i and subtracts value at field
            row i - 1 from it, assigning the value to either the same field or a field with the name new_field
            :param df: the dataframe to process
            :param field: the field to work on
            :param new_field: the name of the new field if any
            :return: the processed dataframe
            """
            if new_field is None:
                new_field = field
            df[new_field] = df[field] - df[field].shift(1)
            df[new_field].fillna(df[field], inplace=True)
            df[new_field].fillna(0, inplace=True)
            df[new_field] = df[new_field].clip(lower=0)

            return df

        df = df[df[COUNTRY_REGION] == country].copy()
        df = subtract(df, CONFIRMED, NEW_CASES)
        # df = subtract(df, DEATHS, 'DeathsTemp')
        # df[DEATHS] = df['DeathsTemp']
        # df = df.drop('DeathsTemp', axis=1) todo decide what you're doing with this

        return df

    def daily_cases_preprocessor(df):
        """
        Processes the dataframe to convert the total confirmed cases to daily new cases
        :param df: the dataframe to work on
        :return: the processed dataframe
        """
        df[NEW_CASES] = 0

        for country in df[COUNTRY_REGION].unique():
            country_df = handle_country_daily_cases(df, country)
            df.replace_rows(COUNTRY_REGION, country, country_df)

        return df

    df = daily_cases_preprocessor(df)
    df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])

    return df


def _load_data(local, github, output):
    """
    Load the daily data into one dataframe
    :param local: the path to the local repo if already cloned
    :param github: the path to the repo to clone/pull from github
    :param output: the output path for the data file
    """
    _log('Loading and processing daily COVID-19 cases data...')
    data_retriever = _get_data_retriever(local, github)
    daily_data = []

    new_data_found = False

    for date in _get_dates():
        df = _load_day_data(date, data_retriever)

        if df is not None:
            new_data_found = True
            daily_data.append(df)

    if new_data_found:
        final_df = pd.concat(daily_data)

        final_df = _preprocess_whole_df(final_df)
        final_df.field_convert(DATE_RECORDED, pd.to_datetime)
        final_df = final_df.sort_values(DATE_RECORDED, ignore_index=True)

        vaccine_data = _load_vaccine_data()
        _log('Merging daily cases data and vaccinations data...')
        merged = pd.merge(final_df, vaccine_data, on=[COUNTRY_REGION, DATE_RECORDED], how='outer')
        merged = merged[merged[COUNTRY_REGION].isin(final_df[COUNTRY_REGION])]

        # TODO decide if you should fill in NAs or just leave them
        """for field in VACCINE_FIELDS[2:]:
            merged[field] = merged[field].fillna(0)"""

        final_df = merged
        final_df = _processing_funcs(final_df)

        if output:
            _log(f'Writing data to {output}...')
            final_df.to_csv(path_or_buf=output, index=False, quoting=csv.QUOTE_NONNUMERIC)

            _log(f'Data written to {output}...')

        return final_df
    else:
        _log('No data to write')
        return None


def _load_vaccine_data():
    """
    Loads the vaccinations data
    :return: vaccinations data
    """
    _log(f'Loading vaccinations data from {VACCINATIONS_FILE}, courtesy of '
          'https://www.kaggle.com/gpreda/covid-world-vaccination-progress...')
    df = pu.from_csv(VACCINATIONS_FILE)
    df.rename({'country': COUNTRY_REGION, 'date': DATE_RECORDED}, inplace=True, axis=1)
    df.field_convert(DATE_RECORDED, pd.to_datetime)
    df = df[VACCINE_FIELDS]

    return df


def _processing_funcs(df):
    """
    Applies the additional processing functions, if any, to the dataframe
    :param df: the dataframe to process
    :return: the processed dataframe
    """
    for func in ADDITIONAL_PROCESSORS:
        df = func(df)

    return df


def current_data_exists():
    """
    Checks if data currently exists and returns true if so
    :return: if data doesn't exist or it is not in a proper format, this will return false
    """
    return os.path.isfile(DATA_FILE) and os.path.isfile(LAST_DATE)


def add_processing_function(func):
    """
    Add a pre-processing function to the module to add additional functionality. The processing functions will be called
    after the data is loaded in fully and after all the pre-defined pre-processing is finished.

    Call this before load_data
    :param func: the processing function
    :return: None
    """
    ADDITIONAL_PROCESSORS.append(func)


def load(local=None, github=None, output=DATA_FILE):
    """
    Load, save and return the processed dataframe
    :param local: the path to the local repo if already cloned
    :param github: the path to the repo to clone/pull from github
    :param output: the output path for the data file. Leave as None if you don't want to write to file
    :return: processed data frame
    """
    _pull_data(local, github, output)

    def drop_rep_ireland(df):
        df = df[df[COUNTRY_REGION] != 'Republic of Ireland']
        return df

    if output and os.path.isfile(output):
        os.remove(output)

    add_processing_function(drop_rep_ireland)
    df = _load_data(local, github, output)
    _log('Finished')

    return df


def main():
    """
    The main entrypoint into this script
    :return: the loaded dataframe
    """
    global _output_messages
    _output_messages = True  # if run from main, output messages to stdin

    local = args.local
    github = args.github
    output = args.output

    return load(local, github, output)


if __name__ == '__main__':
    main()
