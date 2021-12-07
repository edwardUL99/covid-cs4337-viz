"""
This module contains methods used for transforming dataframes for visualising them
"""
from data.fields import DATE_RECORDED, COUNTRY_REGION, WEEK
from data import pandasutils as pu


def map_counts_to_categorical(df, selection_base, columns):
    """
    This method maps counts of the columns in the columns list to a categorical label
    For example, you can use it to map number of cases to a label NewCases, count of deaths
    to label NewDeaths with each row having selection_base + Count + Type columns with Type being the
    label column
    :param df: the dataframe to transform
    :param selection_base: the list of columns to act as a base for selecting the fields
    :param columns: the list of columns to extract and label
    :return: the categorically labelled dataframe
    """
    dataframes = []

    for column in columns:
        dataframes.append((df[selection_base + [column]].copy(), column))

    for subdf, label in dataframes:
        subdf.rename(columns={label: 'Count'}, inplace=True)
        subdf['Type'] = label

    main_df = dataframes[0][0]

    for df1, _ in dataframes[1:]:
        main_df = main_df.append(df1)

    main_df = main_df.drop_duplicates(subset=selection_base + ['Type'], keep='last')

    return main_df


def convert_daily_to_week(df, date_field=DATE_RECORDED, week_number=False):
    """
    Convert the daily dated dataframe to a weekly dated frame using the provided date_field.
    :param df: the dataframe to convert
    :param date_field: the field to convert
    :param week_number: True to show week number of the year instead of date
    :return: the converted dataframe
    """
    return pu.convert_date_field_to_week(df, date_field=date_field, week_number=week_number, inplace=False)\
        .group_aggregate([COUNTRY_REGION, WEEK])


def filter_by_value_and_date(df, value_field, date_field, value, start_date, end_date, multi_value=False):
    """
    Filters the dataframe using date field, the value and start and end dates
    :param df: the dataframe to filter
    :param value_field: the name of the value field to filter by
    :param date_field: the name of the date field to filter by
    :param value: the value to use in filtering
    :param start_date: the start date of the date range
    :param end_date: the end date of the date range
    :param multi_value: if true, the value field will be treated as a list of values
    :return: the filtered dataframe
    """
    if multi_value:
        data = df[df[value_field].isin(value)]
    else:
        data = df[df[value_field] == value]

    data = data[data[date_field] >= start_date]
    data = data[data[date_field] <= end_date]

    return data