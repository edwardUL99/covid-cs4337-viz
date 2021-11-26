"""
This module retrieves the COVID-19 data from the github repository, and persists it in data.csv. By default,
it loads new data if available on every subsequent call to load_data().

It records the last date that was stored by saving it in a file called last-date.txt
"""
import csv

import pandas as pd
import datetime
import os
from sys import argv

import pandasutils as pu
from fields import *

__all__ = ['load_data', 'current_data_exists']

# The base url for the COVID-19 data on github
GITHUB_DATA_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
                  'csse_covid_19_daily_reports/'

# The filename for storing the retrieved data
DATA_FILE = 'data.csv'
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


def _date_to_string(date):
    """
    Converts the date to string
    :param date: the date to convert
    :return: the converted string
    """
    return date.strftime(DATE_FORMAT)


def _create_url(url_base, date, file_extension='csv'):
    """
    Creates the url of the data file to download for the provided date
    :param url_base: the base to append the url onto
    :param date: the date to read the url for
    :param file_extension: the file extension without .
    :return: the created url
    """
    return f'{url_base}{_date_to_string(date)}.{file_extension}'


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

    def load_new_data(self):
        """
        Determine if new data should be loaded
        :return:
        """


class GithubDataRetriever(DataRetriever):
    """
    This class retrieves the data from GitHub
    """

    def __init__(self, github_base_url: str = GITHUB_DATA_URL):
        """
        Creates a GithubDataRetriever with the base url to the repository data
        :param github_base_url: the github url
        """
        self._base = github_base_url

    def retrieve(self, date):
        """
        Retrieve the data frame for this particular day
        :param date: the date to retrieve the dataframe for
        :return: the daily dataframe
        """
        try:
            url = _create_url(self._base, date)
            df = pu.from_csv(url)
        except Exception as e:
            print(e)
            df = None

        return df


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


def _get_data_retriever() -> DataRetriever:
    args = argv[1:]
    args_len = len(args)

    def _parse_next_arg(current_arg):
        index = args.index(current_arg) + 1
        if index < args_len:
            next_arg = args[index]

            return next_arg if not next_arg.startswith('-') else None

    local = 'covid19data'

    if '-local' in args:
        temp = _parse_next_arg('-local')
        local = temp if temp is not None else local
    elif '-github' in args:
        return GithubDataRetriever()

    return LocalDataRetriever(local)


def _get_last_retrieved_date():
    """
    This method gets the date of the data that has been last retrieved. If no data has been retrieved, it uses the first
    date of the data in the github repository
    :return: the date to retrieve data from
    """
    date = FIRST_DATE

    if os.path.isfile(LAST_DATE):
        with open(LAST_DATE, 'r') as file:
            date = file.readlines()[0].strip()

    return datetime.datetime.strptime(date, DATE_FORMAT).date()


def _get_current_df():
    """
    This method reads in the saved data if it exists. If not, this returns None
    :return: the existing data or none if no data exists
    """
    if os.path.isfile(DATA_FILE):
        return pu.from_csv(DATA_FILE, low_memory=False)
    else:
        return None


def _get_dates(last_retrieved_date):
    """
    This is a generator function that yields dates from last_retrieved_date (inclusive) adding one day to the date until
    it reaches the current date
    :param last_retrieved_date: the last date of data retrieved
    :return: None, but it yields dates from last retrieved date to current date, incremented by 1 day
    """
    start_date = last_retrieved_date
    end_date = datetime.datetime.now().date()

    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date = current_date + datetime.timedelta(days=1)


def _write_latest_date(date):
    """
    This writes the date to the last date file
    :param date: the date to write
    :return: None
    """
    with open(LAST_DATE, 'w+') as file:
        file.write(date)


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
        def subtract(df, field, new_field=None):
            if new_field is None:
                new_field = field
            df[new_field] = df[field] - df[field].shift(1)
            df[new_field].fillna(df[field], inplace=True)
            df[new_field].fillna(0, inplace=True)
            df[new_field] = df[new_field].clip(lower=0)

            return df

        df = df[df[COUNTRY_REGION] == country].copy()
        df = subtract(df, CONFIRMED, NEW_CASES)
        #df = subtract(df, DEATHS, 'DeathsTemp')
        #df[DEATHS] = df['DeathsTemp']
        #df = df.drop('DeathsTemp', axis=1)

        return df

    def daily_cases_preprocessor(df):
        df[NEW_CASES] = 0

        for country in df[COUNTRY_REGION].unique():
            country_df = handle_country_daily_cases(df, country)
            df.replace_rows(COUNTRY_REGION, country, country_df)

        return df

    df = daily_cases_preprocessor(df)
    df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])

    return df


def _load_daily_data(load_new_data: bool):
    """
    Load the daily data into one dataframe
    :param load_new_data: true to load in new data if it exists, or false to only read in the current data. If no
    current data exists and load_new_data is false, None will be returned
    :return: a concatenated dataframe of all the daily data
    """
    data_retriever = _get_data_retriever()
    daily_data = []
    current_df = _get_current_df()

    if load_new_data:
        last_date = _get_last_retrieved_date()
        new_data_found = False

        for date in _get_dates(last_date):
            df = _load_day_data(date, data_retriever)

            if df is not None:
                new_data_found = True
                daily_data.append(df)

        if new_data_found:
            if current_df is None:
                current_df = pd.concat(daily_data)
            else:
                current_df = current_df.append(pd.concat(daily_data))

            current_df = _preprocess_whole_df(current_df)
            current_df.field_convert(DATE_RECORDED, pd.to_datetime)
            current_df = current_df.sort_values(DATE_RECORDED, ignore_index=True)

            latest_date = max(current_df[DATE_RECORDED])
            _write_latest_date(_date_to_string(latest_date))

            current_df.to_csv(path_or_buf=DATA_FILE, index=False, quoting=csv.QUOTE_NONNUMERIC)
    else:
        if current_df is not None:
            current_df.field_convert(DATE_RECORDED, pd.to_datetime)

    return current_df


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


def load_data(load_new_data: bool = True) -> pu.DataFrame:
    """
    Loads the data into a single data frame
    :param load_new_data: true to load in new data if it exists, or false to only read in the current data. If no
    current data exists and load_new_data is false, None will be returned
    :return: the dataframe of data if data exists. If load_new_data is false and no data exists, None will be returned
    """
    daily_data = _load_daily_data(load_new_data)
    daily_data = _processing_funcs(daily_data)
    return daily_data


def add_processing_function(func):
    """
    Add a pre-processing function to the module to add additional functionality. The processing functions will be called
    after the data is loaded in fully and after all the pre-defined pre-processing is finished.

    Call this before load_data
    :param func: the processing function
    :return: None
    """
    ADDITIONAL_PROCESSORS.append(func)