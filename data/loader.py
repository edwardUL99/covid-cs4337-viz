"""
This module retrieves the COVID-19 data from the github repository (already pulled to a specified local repo),
and persists it in data.csv. It is intended to be ran as a main script to be used separately to main.py
"""
import argparse
import csv
import os
import sys

import pandas as pd

basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, '..'))

import const
from const import enable_logging, log

import pandasutils as pu
from fields import *

VACCINATIONS_URL = 'https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/global_data/' \
                   'time_series_covid19_vaccine_global.csv'
GITHUB_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
             'csse_covid_19_time_series/'
POPULATIONS_URL = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/' \
                  'WPP2019_TotalPopulationBySex.csv'
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

    def __init__(self, path_or_url: str, on, how: str = 'outer', pre_processor=None, post_processor=None):
        """
        Initialise the custom dataset object
        :param path_or_url: the path or url to the file to read in
        :param on: a string or list of column names to merge on
        :param how: how to perform the merge
        :param pre_processor: an optional processor to operate on the read in data before merging
        :param post_processor: a processing function to process the merged dataframe after merging
        """
        self.path_or_url = path_or_url
        self.on = on
        self.how = how
        self.pre_processor = pre_processor
        self.post_processor = post_processor
        self.df = None

    def read(self):
        """
        Reads in the dataframe and does any necessary processing
        :return: the processed dataframe (also sets self.df
        """
        if self.df is None:
            self.df = pu.from_csv(self.path_or_url)

            if self.pre_processor:
                self.df = self.pre_processor(self.df)

        return self.df

    def merge(self, df):
        """
        Merges the provided df with the df behind this object. The df provided is used as the left dataframe
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
                                             'single DataFrame.')

parser.add_argument('-o', '--output', help='The path to the output file', required=False, default=const.DATA_FILE)
parser.add_argument('-y', '--accept-overwrite', default=False, required=False, help='If --output already exists, '
                                                                                    'overwrite it without prompting',
                    action='store_true')

args = parser.parse_args()
_accept_overwrite = args.accept_overwrite

_output_messages = False


def _prepare_output(output=None):
    """
    Prepares the output directory
    :param output: the output path for the data file
    :return: None
    """

    if output is None:
        output_dir = const.FILE_DIR
    else:
        output_dir = os.path.dirname(output)
        output_dir = os.getcwd() if output_dir == '' else output_dir

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)


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


def _preprocess_whole_df(df: pu.DataFrame):
    """
    Preprocess the final produced dataframe with all daily totals
    :param df: the dataframe to preprocess
    :return: the preprocessed data frame
    """
    df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION, CONFIRMED])
    df = df.drop_duplicates(subset=[DATE_RECORDED, COUNTRY_REGION], keep='last')
    df.subtract_previous(CONFIRMED, COUNTRY_REGION, NEW_CASES)
    df.subtract_previous(DEATHS, COUNTRY_REGION, NEW_DEATHS)

    return df


def _do_merges(df):
    """
    Perform the merges on the CSSE data frame
    :param df: the CSSE data frame
    :return: the merged data frame
    """
    for custom in ADDITIONAL_DATASETS:
        log(f'Merging dataset from {custom.path_or_url}...')
        df = custom.merge(df)

    return df


def _load_data(output):
    """
    Load the daily data into one dataframe
    :param output: the output path for the data file
    """
    log('Loading and processing daily COVID-19 cases data...')
    data_retriever = _get_data_retriever()

    final_df = data_retriever.retrieve()

    final_df = _preprocess_whole_df(final_df)

    log('Merging daily cases data with custom datasets...')
    merged = _do_merges(final_df)
    merged = merged[merged[COUNTRY_REGION].isin(final_df[COUNTRY_REGION])]

    final_df = merged
    final_df = _processing_funcs(final_df)

    final_df = final_df.sort_values(DATE_RECORDED, ignore_index=True)

    if output:
        log(f'Writing data to {output}...')
        final_df.to_csv(path_or_buf=output, index=False, quoting=csv.QUOTE_NONNUMERIC)

        log(f'Data written to {output}...')

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
    log(f'Loading vaccinations data from {VACCINATIONS_URL}')

    def processor(df):
        df.rename({'Country_Region': COUNTRY_REGION, 'Date': DATE_RECORDED, 'Doses_admin': TOTAL_VACCINATIONS},
                  inplace=True, axis=1)
        df.field_convert(DATE_RECORDED, pd.to_datetime)
        df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])
        df = df[VACCINE_FIELDS]

        return df

    def compute_daily_vaccines(df):
        df.subtract_previous(TOTAL_VACCINATIONS, COUNTRY_REGION, DAILY_VACCINATIONS)

        return df

    return CustomDataset(path_or_url=VACCINATIONS_URL, on=[COUNTRY_REGION, DATE_RECORDED], pre_processor=processor,
                         post_processor=compute_daily_vaccines)


def _get_populations_dataset():
    """
    Loads data about populations for each country into the dataset
    :return: the CustomDataset for populations data
    """
    log(f'Loading populations data from {POPULATIONS_URL}')

    def processor(df):
        import datetime

        df = df[['Location', 'Time', 'PopTotal']].copy()
        df['Location'] = df['Location'].map(lambda x: 'US' if x == 'United States of America (and dependencies)' else x)
        df['Time'] = pd.to_datetime(df['Time'], format='%Y')
        df = df[df['Time'].dt.year == datetime.date.today().year].copy()
        df['PopTotal'] = df['PopTotal'].astype('uint32')
        df = df[df['PopTotal'] == df.groupby('Location')['PopTotal'].transform('max')]
        df = df.rename(columns={'Location': COUNTRY_REGION, 'PopTotal': POPULATION})
        df[POPULATION] = df[POPULATION].apply(lambda x: x * 1000)
        df.drop('Time', inplace=True, axis=1)

        return df

    def post_processor(df):
        df = df[~df[POPULATION].isna()].copy()
        df[POPULATION] = df[POPULATION].astype('uint32')
        df = df.group_aggregate([COUNTRY_REGION, DATE_RECORDED])

        return df

    return CustomDataset(path_or_url=POPULATIONS_URL, on=COUNTRY_REGION, pre_processor=processor,
                         post_processor=post_processor)


def calculate_population_metrics(df):
    """
    Calculates per 100000 metrics based off the population field
    :param df: the dataframe to perform calculations on
    :return: the dataframe with calculations calculated on and population column dropped as it is unnecessary
    """
    log('Using population data to calculate population metrics...')

    df[UNVACCINATED] = df[POPULATION] - df[FULLY_VACCINATED]

    fields = [
        {
            'name': CASES_PER_THOUSAND,
            'conversion_field': NEW_CASES
        },
        {
            'name': DEATHS_PER_THOUSAND,
            'conversion_field': NEW_DEATHS
        }
    ]

    for conversion in fields:
        field = conversion['name']
        conversion_field = conversion['conversion_field']
        df[field] = (df[conversion_field] / df[POPULATION]) * 100000
        df[field] = df[field].round(decimals=2)

    df[PERCENTAGE_VACCINATED] = (df[FULLY_VACCINATED] / df[POPULATION]) * 100
    df[PERCENTAGE_VACCINATED] = df[PERCENTAGE_VACCINATED].round(decimals=1)

    df.drop(POPULATION, axis=1, inplace=True)

    return df


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
    add_processing_function(calculate_population_metrics)
    add_custom_dataset(_get_vaccinations_dataset())
    add_custom_dataset(_get_populations_dataset())
    df = _load_data(output)
    log('Finished')

    return df


def main():
    """
    The main entrypoint into this script
    :return: the loaded dataframe
    """
    enable_logging()
    output = args.output
    exists = os.path.isfile(output)

    if not _accept_overwrite and exists:
        confirm = input(f'{output} already exists. Proceeding will overwrite it. Do you wish to proceed? (Y/n)')

        if confirm.strip().lower() != 'n':
            print('Proceeding...')
        else:
            print('Cancelling...')
            exit(0)
    elif exists:
        log(f'{output} exists but Accept Overwrite specified, proceeding...')

    return load(output)


if __name__ == '__main__':
    main()
