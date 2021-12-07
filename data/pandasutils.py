""""
This module provides utilities for working with pandas DataFrames.

You can import this module as
import pandasutils as pu
"""
import numpy as np
import pandas as pd


class DataFrame(pd.DataFrame):
    """
    This class extends the pandas DataFrame to provide some utility functions that aren't provided by the pandas DataFrame api
    """
    def __init__(self, *args, **kwargs):
        """
        Constructs the dataframe with the provided arguments and key word arguments
        :param args: the arguments passed into the constructor
        :param kwargs: the keyword arguments passed into the constructor
        """
        super().__init__(*args, **kwargs)

    @property
    def _constructor(self):
        """
        Override panda's constructor so it knows which to construct
        :return:
        """
        return DataFrame

    def group_aggregate(self, group_by, avg=False):
        """
        This function groups by the provided group_by fields and then aggregates using the sum function.

        For example, the following DataFrame
        date          id       price
        2020-01-01     1       20.00
        2020-01-01     1       22.00
        2020-01-01     1       23.00
        2020-01-02     1       10.00

        and you call group_aggregate(['date, id']), you get the following:
        date         id        price
        2020-01-01    1        65.00
        2020-01-02    1        10.00
        :param group_by: the field(s) to group by
        :param avg: average the field instead of sum
        :return: the aggregated dataframe
        """
        group_by = self.groupby(group_by)

        if avg:
            group_by = group_by.mean()
        else:
            group_by = group_by.sum()

        return group_by.reset_index()

    def fill_required(self, required_fields, rename_mapper=None):
        """
        This fills required fields with Na if they are not found. If rename_mapper is not None, it will be passed into
        rename before the required_fields are processed
        :param df: the dataframe to process
        :param required_fields: the fields that are required to be present, and fill with NA if not found
        :param rename_mapper: a mapper to rename columns before the required_fields are checked. If None, the processing is
        done without renaming any columns
        :return: the processed dataframe
        """
        if rename_mapper is not None:
            df = self.rename(columns=rename_mapper)
        else:
            df = self.copy()

        fields_in_df = [field for field in required_fields if field in df.columns]
        fields_not_in_df = [field for field in required_fields if field not in fields_in_df]
        preprocessed_df = df[fields_in_df]

        if len(fields_not_in_df) > 0:
            for field in fields_not_in_df:
                preprocessed_df[field] = pd.NA

        return preprocessed_df

    def subtract_previous(self, field, group_field, new_field=None, inplace=True):
        """
        With the given field, this method takes value at field row i and subtracts value at field
        row i - 1 from it, assigning the value to either the same field or a field with the name new_field
        :param field: the field to work on
        :param group_field: the field to group by and do subtraction by
        :param new_field: the name of the new field if any
        :param inplace: true to perform in place, else on a copy
        :return: the processed dataframe if not in place or none if inplace
        """
        if inplace:
            df = self
        else:
            df = self.copy()

        df[new_field] = df[field] - df.groupby(group_field)[field].shift(1)
        df[new_field] = df[new_field].clip(lower=0)

        return df if not inplace else None

    def field_convert(self, field, converter_func, inplace=True, **kwargs):
        """
        Takes the provided field and passes it through the converter_func.
        An example use case is, field_convert('DateField', pd.to_datetime) to convert a string date to a datetime.
        This essentially calls self[field] = converter_func(self[field], **kwargs)
        :param field: the field to convert
        :param converter_func: the function to perform the conversion
        :param inplace: True to convert the field in place or to return a copy
        :param kwargs: arguments to pass to converter_func
        :return: None if inplace is True, else the copied data frame with the converted field
        """
        if inplace:
            df = self
        else:
            df = self.copy()

        df[field] = converter_func(df[field], **kwargs)

        return None if inplace else df

    def create_column(self, column_name: str, data_or_producer_func=pd.NA, inplace=True, overwrite=True):
        """
        Create a new column with the provided column name and data or function that produces the data
        :param column_name: the name of the new column. If it already exists, the column will be overwritten if overwrite
        is True, else an exception will be thrown
        :param data_or_producer_func: a list of data or a function that produces the data. Default is pandas NA
        :param inplace: true to add the column to this dataframe, or a copy
        :param overwrite: true to overwrite a column if it already exists. If false, an error is thrown
        :return: None if inplace is true, else the constructed copy
        """
        df = self if inplace else self.copy()

        if column_name in df and not overwrite:
            raise ValueError(f'The column {column_name} already exists and overwrite was not requested,'
                             ' cannot perform the operation')

        data = data_or_producer_func() if callable(data_or_producer_func) else data_or_producer_func

        df[column_name] = data

        return None if inplace else df

    def replace_rows(self, column, value, replacement, inplace=True):
        """
        Replaces rows which has columns matching a provided value with the replacement dataframe.
        The column in the replacement dataframe should have all values the same as the specified value
        and no other value, (i.e. the value should be distinct)
        :param column: the name of the column on which replacement should occur
        :param value: the value the column should have to be replaced
        :param replacement: the replacement dataframe
        :param inplace: true to perform on this dataframe, false to perform on a copy
        :return: the copied dataframe if inplace is false, else none
        """
        other_values = replacement[replacement[column] != value]

        if not other_values.empty:
            raise ValueError(f'The replacement DataFrame has rows which have values different to {value}')

        df = self if inplace else self.copy()

        df.loc[df[column] == value] = replacement.values

        return None if inplace else df

    @classmethod
    def from_pandas(cls, df, convert_datetime=None, datetime_format=None):
        """
        Constructs a pandasutils.DataFrame from the pandas df
        :param df: the pandas dataframe to construct from
        :param convert_datetime: an optional name of a field to convert to pandas datetime
        :param datetime_format: an optional format for the datetime
        :return: the utils DataFrame
        """
        data = {}

        for col in df.columns:
            data[col] = df[col]

        data = cls(data)

        if convert_datetime:
            if datetime_format:
                data[convert_datetime] = pd.to_datetime(data[convert_datetime], format=datetime_format)
            else:
                data[convert_datetime] = pd.to_datetime(data[convert_datetime])

        return data

    @classmethod
    def from_csv(cls, csv_file, convert_datetime=None, datetime_format=None, **kwargs):
        """
        Constructs the dataframe from the provided csv file
        :param csv_file: the csv file to construct the dataframe from
        :param convert_datetime: an optional name of a field to convert to pandas datetime
        :param datetime_format: an optional format for the datetime
        :param kwargs: keyword arguments to pass into read_csv
        :return: the constructed dataframe
        """
        df = pd.read_csv(csv_file, **kwargs)

        return DataFrame.from_pandas(df, convert_datetime=convert_datetime, datetime_format=datetime_format)


def from_csv(csv_file, convert_datetime=None, datetime_format=None, **kwargs) -> DataFrame:
    """
    A utility function for calling
    pu.DataFrame.from_csv(csv_file)

    :param csv_file: the CSV file to read from
    :param kwargs: key word arguments to pass into the csv method
    :param convert_datetime: an optional name of a field to convert to pandas datetime
    :param datetime_format: an optional format for the datetime
    :return: the constructed dataframe
    """
    return DataFrame.from_csv(csv_file, convert_datetime=convert_datetime, datetime_format=datetime_format, **kwargs)


def from_pandas(df, convert_datetime=None, datetime_format=None) -> DataFrame:
    """
    A utility function for calling pu.DataFrame.from_pandas(df)
    :param df: the pandas dataframe
    :param convert_datetime: an optional name of a field to convert to pandas datetime
    :param datetime_format: an optional format for the datetime
    :return: the utils dataframe
    """
    return DataFrame.from_pandas(df, convert_datetime=convert_datetime, datetime_format=datetime_format)


def convert_date_field_to_week(df: DataFrame, date_field: str, inplace=True, week_number=True):
    """
    A utility function to convert a date field that may be a day and group it down into a week
    :param df: the dataframe to convert the date field to week
    :param date_field: the name of the date field to convert
    :param inplace: true to provide the operation on the provided date_field, false to perform it on a copy
    :param week_number: true if you want year-week number. False if you want the date of the start of the week
    :return: the dataframe with an added 'Week' column if not inplace, or none if inplace
    """
    def data_func():
        if week_number:
            return df[date_field].dt.strftime('%Y-%U')
        else:
            return df[date_field] - df[date_field].dt.weekday * np.timedelta64(1, 'D')

    return df.create_column('Week', data_func, inplace=inplace)