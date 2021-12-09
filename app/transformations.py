"""
This module contains methods used for transforming dataframes for visualising them
"""
from data.fields import DATE_RECORDED, COUNTRY_REGION, WEEK, NUMBER_DETECTIONS_VARIANT, VARIANT, \
    PERCENT_VARIANT, VARIANT_FIELDS, NEW_CASES, NEW_DEATHS, CASES_PER_THOUSAND, DEATHS_PER_THOUSAND, \
    DAILY_TESTS, POSITIVE_RATE, TOTAL_BOOSTERS, BOOSTERS_PER_HUNDRED
from data import pandasutils as pu


def map_counts_to_categorical(df, selection_base, columns, label_mappings=None, keep_max_value=True):
    """
    This method maps counts of the columns in the columns list to a categorical label
    For example, you can use it to map number of cases to a label NewCases, count of deaths
    to label NewDeaths with each row having selection_base + Count + Type columns with Type being the
    label column
    :param df: the dataframe to transform
    :param selection_base: the list of columns to act as a base for selecting the fields
    :param columns: the list of columns to extract and label
    :param label_mappings: an optional map to map the field name to a more friendly label name
    :param keep_max_value: drops duplicates by keeping the maximum value
    :return: the categorically labelled dataframe
    """
    def keep_max(df1, group_by, field):
        maximum = df1.groupby(group_by)[field].max()
        maximum = max(maximum)

        return df1[df1[field] == maximum]

    if label_mappings is None:
        label_mappings = {}

    dataframes = [(df[selection_base + [column]].copy(), column) for column in columns]

    for subdf, label in dataframes:
        subdf.rename(columns={label: 'Count'}, inplace=True)
        subdf['Type'] = label_mappings.get(label, label)

    main_df = dataframes[0][0]
    main_df = keep_max(main_df, selection_base + ['Type'], 'Count') if keep_max_value else main_df

    for df1, _ in dataframes[1:]:
        df1 = keep_max(df1, selection_base + ['Type'], 'Count') if keep_max_value else df1
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


def get_variants_data(df):
    """
    Retrieve the variants data from the loaded dataframe
    :param df: the dataframe to process
    :return: the processed dataframe having variant data
    """
    df = df.dropna(subset=[NUMBER_DETECTIONS_VARIANT, VARIANT, PERCENT_VARIANT])

    return df


def get_variants_sums(df):
    """
    Get the summation of variants detected grouped by week and variant
    :param df: the dataframe to sum
    :return: the processed dataframe
    """
    df = df.dropna(subset=VARIANT_FIELDS)
    df = df.groupby([DATE_RECORDED, VARIANT])[NUMBER_DETECTIONS_VARIANT].sum()
    df = df.reset_index()

    return df


def compute_variant_proportions(df):
    """
    Compute the proportions of the variants
    :param df: the dataframe to compute from
    :return: the processed dataframe
    """
    df = df.dropna(subset=VARIANT_FIELDS)
    df = df.groupby([VARIANT])[NUMBER_DETECTIONS_VARIANT].sum()
    df = df.reset_index()

    return df


def compute_monthly_cases_deaths(df, by_thousand):
    """
    Compute cases and deaths by month
    :param df: the dataframe to process
    :param by_thousand: true to display cases and deaths by 1000
    :return: the processed dataframe and the labels used
    """
    import pandas as pd

    cases_deaths_fields = [CASES_PER_THOUSAND, DEATHS_PER_THOUSAND] if by_thousand else [NEW_CASES, NEW_DEATHS]

    df = df[[COUNTRY_REGION, DATE_RECORDED] + cases_deaths_fields].copy()
    df['Month'] = df[DATE_RECORDED].apply(lambda x: f'{x.month}-{x.year}')
    df['Month'] = pd.to_datetime(df['Month'], format='%m-%Y')
    df = df.group_aggregate([COUNTRY_REGION, 'Month'], avg=True)
    df.sort_values(by='Month', inplace=True)

    df = map_counts_to_categorical(df, [COUNTRY_REGION, 'Month'], cases_deaths_fields, keep_max_value=False)

    return df, cases_deaths_fields


def compute_testing_metrics(df):
    """
    Create a dataframe with testing metrics
    :param df: the dataframe to process
    :return: the processed dataframe
    """
    pu.convert_date_field_to_week(df, DATE_RECORDED, week_number=False)
    data = df.group_aggregate([COUNTRY_REGION, WEEK], avg=True)
    data['PositiveTests'] = data[DAILY_TESTS] * data[POSITIVE_RATE]
    positive_rate = data[POSITIVE_RATE] * 100
    data = map_counts_to_categorical(data, [COUNTRY_REGION, WEEK], [DAILY_TESTS, 'PositiveTests'],
                                     keep_max_value=False,
                                     label_mappings={
                                         DAILY_TESTS: 'Tests Taken',
                                         'PositiveTests': 'Positive Tests'
                                     })

    data[POSITIVE_RATE] = positive_rate

    return data


def get_vaccination_percentage_and_boosters(df):
    """
    Get the dataframe for vaccination percentages and boosters given
    :param df: the dataframe to process
    :return: processed dataframes, 1 for vaccination percentage and another for boosters
    """
    pu.convert_date_field_to_week(df, DATE_RECORDED, week_number=False)
    df = df.group_aggregate([COUNTRY_REGION, WEEK], avg=True)

    vaccination_data = df.copy()
    boosters_data = df.copy()

    boosters_data[TOTAL_BOOSTERS] = boosters_data[BOOSTERS_PER_HUNDRED]
    boosters_data = boosters_data[boosters_data[TOTAL_BOOSTERS] > 0].copy()

    return vaccination_data, boosters_data
