"""
This module retrieves the COVID-19 data from the github repository (already pulled to a specified local repo),
and persists it in data.csv. It is intended to be ran as a main script to be used separately to main.py
"""
import csv
import argparse

import pandas as pd
import os

import const

basedir = os.path.abspath(os.path.dirname(__file__))

import pandasutils as pu
from fields import *

VACCINATIONS_URL = 'https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/global_data/' \
                   'time_series_covid19_vaccine_global.csv'
GITHUB_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
             'csse_covid_19_time_series/'
# The format for parsing dates
DATE_FORMAT = '%m/%d/%y'
# The fields of the downloaded data we want to keep
FIELDS_TO_KEEP = ALL_FIELDS

# Additional preprocessors to add while loading data
ADDITIONAL_PROCESSORS = []


class CustomDataset:
    """
    This class represents an additional dataset to merge into the main CSSE dataframe
    """
    def __init__(self, path_or_url: str, on, how: str = 'outer', data_processor=None, post_processor=None):
        """
        Initialise the custom dataset object
        :param path_or_url: the path or url to the file to read in
        :param on: a string or list of column names to merge on
        :param how: how to perform the merge
        :param data_processor: an optional processor to operate on the read in data
        :param post_processor: a processing function to process the merged dataframe after merging
        """
        self.path_or_url = path_or_url
        self.on = on
        self.how = how
        self.data_processor = data_processor
        self.post_processor = post_processor
        self.df = None

    def read(self):
        """
        Reads in the dataframe and does any necessary processing
        :return: the processed dataframe (also sets self.df
        """
        if self.df is None:
            self.df = pu.from_csv(self.path_or_url)

            if self.data_processor:
                self.df = self.data_processor(self.df)

        return self.df

    def merge(self, df):
        """
        Merges the provided def with the df behind this object. The df provided is used as the left dataframe
        :param df: the left dataframe to merge
        :return: the merged dataframe
        """
        right_df = self.read()
        merged = pd.merge(df, right_df, on=self.on, how=self.how)

        if self.post_processor:
            merged = self.post_processor(merged)

        return merged


# additional datasets to merge into the CSSE data
ADDITIONAL_DATASETS: list[CustomDataset] = []

parser = argparse.ArgumentParser(description='Pulls and cleans CSSE GIS Covid-19 data and vaccination data into a '
                                             'single DataFrame. By default, it pulls data into the local directory '
                                             'under the data directory')

parser.add_argument('-o', '--output', help='The path to the output file', required=False, default=const.DATA_FILE)

args = parser.parse_args()
_output_messages=False


def _log(msg):
    if _output_messages:
        print(msg)


def _prepare_output(output=None):
    """
    Prepares the output directory
    :param output: the output path for the data file
    :return: None
    """

    if output is None:
        output = const.FILE_DIR
    else:
        output = os.path.dirname(output)

    if not os.path.isdir(output):
        os.makedirs(output)


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

    def retrieve(self):
        """
        Retrieve the data frame for this particular day
        :return: the daily dataframe
        """
        raise NotImplementedError


class GithubDataRetriever(DataRetriever):
    """
    This class retrieves data from github
    """

    def __init__(self):
        """
        Create a LocalDataRetriever
        """
        super().__init__()

    def retrieve(self):
        """
        Retrieve the data frame for this particular day
        :return: the daily dataframe
        """
        confirmed_url = f'{GITHUB_URL}time_series_covid19_confirmed_global.csv'
        deaths_url = f'{GITHUB_URL}time_series_covid19_deaths_global.csv'
        recovered_url = f'{GITHUB_URL}time_series_covid19_recovered_global.csv'

        def _melt(df, value_name):
            """
            Melt wide form into long form
            :param df: the dataframe to melt
            :param value_name: the name of the value column
            :return: the melted dataframe
            """
            dates = df.columns[4:]

            df_long = df.melt(
                id_vars=[COUNTRY_REGION],
                value_vars=dates,
                var_name=DATE_RECORDED,
                value_name=value_name
            )

            return df_long

        df_confirmed = _melt(pu.from_csv(confirmed_url), CONFIRMED)
        df_deaths = _melt(pu.from_csv(deaths_url), DEATHS)
        df_recovered = _melt(pu.from_csv(recovered_url), RECOVERED)

        full_df = df_confirmed.merge(
            right=df_deaths,
            how='left',
            on=[COUNTRY_REGION, DATE_RECORDED]
        )

        full_df = full_df.merge(
            right=df_recovered,
            how='left',
            on=[COUNTRY_REGION, DATE_RECORDED]
        )

        full_df.field_convert(DATE_RECORDED, pd.to_datetime, format=DATE_FORMAT)

        return full_df


def _get_data_retriever() -> DataRetriever:
    """
    Set up the DataRetriever for use to read in the csse covid 19 data
    :return: the data retriever to retrieve the data
    """

    return GithubDataRetriever()


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

        df = df[df[COUNTRY_REGION] == country].copy()
        df.subtract_previous(CONFIRMED, NEW_CASES)
        df.subtract_previous(DEATHS, NEW_DEATHS)

        return df

    def daily_cases_preprocessor(df):
        """
        Processes the dataframe to convert the total confirmed cases to daily new cases
        :param df: the dataframe to work on
        :return: the processed dataframe
        """
        df[NEW_CASES] = 0
        df[NEW_DEATHS] = 0

        for country in df[COUNTRY_REGION].unique():
            country_df = handle_country_daily_cases(df, country) # TODO for US data, it seems very erratic and differing new cases numbers compared to https://91-divoc.com/pages/covid-visualization/
            # TODO check out this article: https://towardsdatascience.com/covid-19-data-processing-58aaa3663f6, think about using time series instead
            df.replace_rows(COUNTRY_REGION, country, country_df)

        return df

    df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])
    df = daily_cases_preprocessor(df)

    return df


def _do_merges(df):
    """
    Perform the merges on the CSSE data frame
    :param df: the CSSE data frame
    :return: the merged data frame
    """
    for custom in ADDITIONAL_DATASETS:
        _log(f'Merging dataset from {custom.path_or_url}...')
        df = custom.merge(df)

    return df


def _load_data(output):
    """
    Load the daily data into one dataframe
    :param output: the output path for the data file
    """
    _log('Loading and processing daily COVID-19 cases data...')
    data_retriever = _get_data_retriever()

    final_df = data_retriever.retrieve()

    final_df = _preprocess_whole_df(final_df)
    final_df.field_convert(DATE_RECORDED, pd.to_datetime)
    final_df = final_df.sort_values(DATE_RECORDED, ignore_index=True)

    # vaccine_data = _load_vaccine_data()
    _log('Merging daily cases data with custom datasets...')
    merged = _do_merges(final_df)
    merged = merged[merged[COUNTRY_REGION].isin(final_df[COUNTRY_REGION])]

    final_df = merged
    final_df = _processing_funcs(final_df)

    if output:
        _log(f'Writing data to {output}...')
        final_df.to_csv(path_or_buf=output, index=False, quoting=csv.QUOTE_NONNUMERIC)

        _log(f'Data written to {output}...')

    return final_df


def _processing_funcs(df):
    """
    Applies the additional processing functions, if any, to the dataframe
    :param df: the dataframe to process
    :return: the processed dataframe
    """
    for func in ADDITIONAL_PROCESSORS:
        df = func(df)

    return df


def add_processing_function(func):
    """
    Add a pre-processing function to the module to add additional functionality. The processing functions will be called
    after the data is loaded in fully and after all the pre-defined pre-processing is finished.

    Call this before load_data
    :param func: the processing function
    :return: None
    """
    ADDITIONAL_PROCESSORS.append(func)


def add_custom_dataset(custom: CustomDataset):
    """
    Adds a custom dataset to merge into the CSSE dataset. Multiple custom datasets are merged as a chain in the order
    that they are added
    :param custom: the custom dataset
    :return: None
    """
    ADDITIONAL_DATASETS.append(custom)


def _get_vaccinations_dataset():
    """
    Define the CustomDataset for vaccinations data and return it
    :return: CustomDataset for vaccinations data
    """
    _log(f'Loading vaccinations data from {VACCINATIONS_URL}')

    def processor(df):
        df.rename({'Country_Region': COUNTRY_REGION, 'Date': DATE_RECORDED, 'Doses_admin': TOTAL_VACCINATIONS}, inplace=True, axis=1)
        df.field_convert(DATE_RECORDED, pd.to_datetime)
        df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])
        df = df[VACCINE_FIELDS]

        return df

    def compute_daily_vaccines(df):
        df[DAILY_VACCINATIONS] = 0

        for country in df[COUNTRY_REGION].unique():
            country_df = df[df[COUNTRY_REGION] == country].copy()
            country_df.subtract_previous(TOTAL_VACCINATIONS, DAILY_VACCINATIONS)
            df.replace_rows(COUNTRY_REGION, country, country_df)

        return df

    return CustomDataset(path_or_url=VACCINATIONS_URL, on=[COUNTRY_REGION, DATE_RECORDED], data_processor=processor,
                         post_processor=compute_daily_vaccines)


def load(output=const.DATA_FILE):
    """
    Load, save and return the processed dataframe
    :param output: the output path for the data file. Leave as None if you don't want to write to file
    :return: processed data frame
    """
    _prepare_output(output)

    def drop_rep_ireland(df):
        df = df[df[COUNTRY_REGION] != 'Republic of Ireland']
        return df

    if output and os.path.isfile(output):
        os.remove(output)

    add_processing_function(drop_rep_ireland)
    add_custom_dataset(_get_vaccinations_dataset())
    df = _load_data(output)
    _log('Finished')

    return df


def main():
    """
    The main entrypoint into this script
    :return: the loaded dataframe
    """
    global _output_messages
    _output_messages = True  # if run from main, output messages to stdin

    output = args.output

    if output and os.path.isfile(output):
        confirm = input(f'{output} already exists. Proceeding will overwrite it. Do you wish to proceed? (Y/n)')

        if confirm.strip().lower() != 'n':
            print('Proceeding...')
        else:
            print('Cancelling...')
            exit(0)

    return load(output)


if __name__ == '__main__':
    main()
