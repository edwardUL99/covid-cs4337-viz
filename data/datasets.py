"""
This module contains functionality for defining additional datasets
"""
import pandas as pd
import pandasutils as pu

from fields import *
from const import log


VACCINATIONS_URL = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/' \
                   'vaccinations.csv'
GITHUB_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
             'csse_covid_19_time_series/'
POPULATIONS_URL = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/' \
                  'WPP2019_TotalPopulationBySex.csv'
VARIANT_DATA = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/variants/covid-variants.csv'
TESTING_DATA = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/' \
               'covid-testing-all-observations.csv'

custom_datasets = []


class CustomDataset:
    """
    This class represents an additional dataset to merge into the main CSSE dataframe
    """

    def __init__(self, path_or_url: str, on, how: str = 'outer', pre_processor=None, post_processor=None, **kwargs):
        """
        Initialise the custom dataset object
        :param path_or_url: the path or url to the file to read in
        :param on: a string or list of column names to merge on
        :param how: how to perform the merge
        :param pre_processor: an optional processor to operate on the read in data before merging
        :param post_processor: a processing function to process the merged dataframe after merging
        :param kwargs: arguments to pass into the function reading the dataframe
        """
        self.path_or_url = path_or_url
        self.on = on
        self.how = how
        self.pre_processor = pre_processor
        self.post_processor = post_processor
        self.df = None
        self.pandas_args = kwargs

    def read(self):
        """
        Reads in the dataframe and does any necessary processing
        :return: the processed dataframe (also sets self.df
        """
        if self.df is None:
            self.df = pu.from_csv(self.path_or_url, **self.pandas_args)

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


class Producer:
    """
    This class represents a decorator to register the fact that a function is defined as a CustomDataset producer
    """
    def __init__(self, func):
        self.func = func
        custom_datasets.append(self.func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


@Producer
def _get_vaccinations_dataset():
    """
    Define the CustomDataset for vaccinations data and return it
    :return: CustomDataset for vaccinations data
    """
    log(f'Loading vaccinations data from {VACCINATIONS_URL}')

    def processor(df):
        df.rename({'location': COUNTRY_REGION, 'date': DATE_RECORDED, 'total_vaccinations': TOTAL_VACCINATIONS},
                  inplace=True, axis=1)
        df.field_convert(DATE_RECORDED, pd.to_datetime, format='%Y-%m-%d')
        df[DATE_RECORDED] = df[DATE_RECORDED].dt.floor('d')
        df = df.group_aggregate([DATE_RECORDED, COUNTRY_REGION])
        df[PARTIALLY_VACCINATED] = df['people_vaccinated'] - df['people_fully_vaccinated']
        df[COUNTRY_REGION] = df[COUNTRY_REGION].apply(lambda x: 'US' if x == 'United States' else x)
        df = df[VACCINE_FIELDS]

        return df

    return CustomDataset(path_or_url=VACCINATIONS_URL, on=[COUNTRY_REGION, DATE_RECORDED], pre_processor=processor)


@Producer
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


@Producer
def _get_variant_dataset():
    """
    Get the data for the Covid-19 variants.
    :return: custom dataset for variants
    """
    log(f'Loading Covid-19 Variants data from {VARIANT_DATA}')

    def processor(df):
        df = df.rename(columns={'location': COUNTRY_REGION, 'date': DATE_RECORDED,
                                'num_sequences': NUMBER_DETECTIONS_VARIANT, 'perc_sequences': PERCENT_VARIANT})
        df = df[VARIANT_FIELDS].copy()
        df[DATE_RECORDED] = pd.to_datetime(df[DATE_RECORDED], format='%Y-%m-%d')
        df[DATE_RECORDED] = df[DATE_RECORDED].dt.floor('d')
        df[COUNTRY_REGION] = df[COUNTRY_REGION].apply(lambda x: 'US' if x == 'United States' else x)

        def variant_mapper(variant):
            if variant.startswith('B') or variant.startswith('S') or variant == 'non_who':
                return 'Unknown'
            else:
                return variant

        df[VARIANT] = df[VARIANT].apply(variant_mapper)

        return df

    return CustomDataset(path_or_url=VARIANT_DATA, on=[COUNTRY_REGION, DATE_RECORDED], pre_processor=processor)


@Producer
def _get_testing_dataset():
    """
    Retrieves the dataset for testing
    :return: the testing dataset
    """
    log(f'Loading Covid-19 testing data from {TESTING_DATA}')

    def processor(df):
        df = df.rename(columns={'Entity': COUNTRY_REGION, 'Date': DATE_RECORDED,
                                'Daily change in cumulative total': DAILY_TESTS,
                                'Cumulative total': TOTAL_TESTS,
                                'Short-term positive rate': POSITIVE_RATE})
        df[DATE_RECORDED] = pd.to_datetime(df[DATE_RECORDED], format='%Y-%m-%d')
        df[DATE_RECORDED] = df[DATE_RECORDED].dt.floor('d')

        def country_mapper(country):
            country = country[:country.index('-')].strip()
            return 'US' if country == 'United States' else country

        df[COUNTRY_REGION] = df[COUNTRY_REGION].apply(country_mapper)
        df = df[TESTING_FIELDS].copy()

        return df

    return CustomDataset(path_or_url=TESTING_DATA, on=[COUNTRY_REGION, DATE_RECORDED], pre_processor=processor)


def merge(df):
    """
    Merge the datasets defined by @Producer into the provided dataframe
    :param df: the dataframe to merge into
    :return: the fully merged dataframe
    """
    for custom in custom_datasets:
        dataset = custom()
        log(f'Merging dataset from {dataset.path_or_url}...')
        df = dataset.merge(df)

    return df
