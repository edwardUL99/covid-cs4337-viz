"""
This module contains functionality for defining additional datasets
"""
import pandas as pd
import pandasutils as pu
import yaml

from fields import *
from const import log


VACCINATIONS_URL = 'https://raw.githubusercontent.com/govex/COVID-19/master/data_tables/vaccine_data/global_data/' \
                   'time_series_covid19_vaccine_global.csv'
GITHUB_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/' \
             'csse_covid_19_time_series/'
POPULATIONS_URL = 'https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/' \
                  'WPP2019_TotalPopulationBySex.csv'
EU_VARIANT_DATA = 'https://opendata.ecdc.europa.eu/covid19/virusvariant/csv/data.csv'


with open('variants.yaml') as f:
    VARIANT_NAMES: dict = yaml.safe_load(f)


custom_datasets = []


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
def _get_eu_variant_dataset():
    """
    Get the data for the Covid-19 variants in the EU.
    :return: custom dataset for EU variants
    """
    log(f'Loading EU Covid-19 Variants data from {EU_VARIANT_DATA}')

    def processor(df):
        df = df.rename(columns={'country': COUNTRY_REGION, 'year_week': DATE_RECORDED, VARIANT: LINEAGE})
        df = df[VACCINE_FIELDS].copy()
        df[DATE_RECORDED] = df[DATE_RECORDED].apply(lambda x: x + '-1')
        df[DATE_RECORDED] = pd.to_datetime(df[DATE_RECORDED], format='%Y-%W-%w')

        def variant_mapper(variant):
            return VARIANT_NAMES.get(variant, 'Unknown')

        df[VARIANT] = df[LINEAGE].apply(variant_mapper)

        return df

    return CustomDataset(path_or_url=EU_VARIANT_DATA, on=[COUNTRY_REGION, DATE_RECORDED], pre_processor=processor)


def get_datasets() -> list[CustomDataset]:
    """
    Return the defined datasets in this module
    :return: list of defined datasets
    """
    return [dataset() for dataset in custom_datasets]
