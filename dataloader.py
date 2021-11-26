"""
This module retrieves the COVID-19 data from the github repository (already pulled to a specified local repo),
and persists it in data.csv. It is intended to be ran as a main script to be used separately to main.py
"""
import csv

import pandas as pd
import datetime
import os
from sys import argv

import pandasutils as pu
from fields import *

# The filename for storing the retrieved data
DATA_FILE = 'data.csv'
VACCINATIONS_FILE = 'country_vaccinations.csv'
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


def _get_data_retriever() -> DataRetriever:
    """
    Set up the DataRetriever for use to read in the csse covid 19 data.
    If this script has been run with the argument -local with the path to the repository storing the CSSE data
    on your local machine, the specified path will be used, else default is just covid19data (i.e. checkout the repo
    as covid19data in the working directory)
    :return: the data retriever to retrieve the data
    """
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

    return LocalDataRetriever(local)


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
        #df = subtract(df, DEATHS, 'DeathsTemp')
        #df[DEATHS] = df['DeathsTemp']
        #df = df.drop('DeathsTemp', axis=1) todo decide what you're doing with this

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


def _load_daily_data():
    """
    Load the daily data into one dataframe
    :param load_new_data: true to load in new data if it exists, or false to only read in the current data. If no
    current data exists and load_new_data is false, None will be returned
    """
    print('Loading and processing daily COVID-19 cases data...')
    data_retriever = _get_data_retriever()
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
        print('Merging daily cases data and vaccinations data...')
        merged = pd.merge(final_df, vaccine_data, on=[COUNTRY_REGION, DATE_RECORDED], how='outer')
        merged = merged[merged[COUNTRY_REGION].isin(final_df[COUNTRY_REGION])]

        # TODO decide if you should fill in NAs or just leave them
        """for field in VACCINE_FIELDS[2:]:
            merged[field] = merged[field].fillna(0)"""

        final_df = merged
        final_df = _processing_funcs(final_df)
        print(f'Writing data to {DATA_FILE}...')
        final_df.to_csv(path_or_buf=DATA_FILE, index=False, quoting=csv.QUOTE_NONNUMERIC)

        print(f'Data written to {DATA_FILE}...')

        return final_df
    else:
        print('No data to write')
        return None


def _load_vaccine_data():
    """
    Loads the vaccinations data
    :return: vaccinations data
    """
    print(f'Loading vaccinations data from {VACCINATIONS_FILE}, courtesy of '
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


def load_data():
    """
    Loads the data into a single data frame
    """
    return _load_daily_data()


def add_processing_function(func):
    """
    Add a pre-processing function to the module to add additional functionality. The processing functions will be called
    after the data is loaded in fully and after all the pre-defined pre-processing is finished.

    Call this before load_data
    :param func: the processing function
    :return: None
    """
    ADDITIONAL_PROCESSORS.append(func)


def load():
    """
    Load, save and return the processed dataframe
    :return: processed data frame
    """
    def drop_rep_ireland(df):
        df = df[df[COUNTRY_REGION] != 'Republic of Ireland']
        return df

    if os.path.isfile(f'{DATA_FILE}'):
        os.remove(DATA_FILE)

    add_processing_function(drop_rep_ireland)
    df = load_data()
    print('Finished')

    return df


def main():
    """
    The main entrypoint into this script
    :return: the loaded dataframe
    """
    return load()


if __name__ == '__main__':
    main()
