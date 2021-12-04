import datetime
import os
import sys
import argparse

basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, '..'))

import dash
import pandas as pd
from dash import html
from dash import dcc
from dash.dependencies import Input, Output

from plotly.subplots import make_subplots

import dashutils as du
from data import pandasutils as pu
from data.fields import *

import const
from const import enable_logging, log

from enum import Enum

enable_logging(__name__ == '__main__')

app = dash.Dash(__name__,
                external_stylesheets=['https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css'])


parser = argparse.ArgumentParser(description='The server for providing interactive COVID-19 visualisations')
parser.add_argument('-f', '--file', default=const.DATA_FILE, required=False, help='The path to the data.csv file'
                                                                                    'generated by data.py')
args = parser.parse_args()


DATA_FILE = args.file


# TODO look at this website for visualisation ideas: https://ourworldindata.org/coronavirus/country/united-states


def get_data():
    """
    Loads the data in from data.csv. If it doesn't exist, it will attempt to bootstrap by executing the data.py
    script (therefore, any command line arguments that script requires needs to be passed to this script). Otherwise,
    run the script separately before starting the server
    :return: the loaded DataFrame
    """
    if not os.path.isfile(DATA_FILE):
        print(f'{DATA_FILE} does not exist. Run data.py before calling this script')
        exit(1)
    else:
        log(f'Loading data from {DATA_FILE} into a DataFrame')
        df = pu.from_csv(DATA_FILE)
        df.field_convert(DATE_RECORDED, pd.to_datetime)

        return df


log('Starting')
df = get_data()
country_dropdown = du.ColumnDropdown(df, COUNTRY_REGION, className='w-50')
country_dropdown_multiple = du.ColumnDropdown(df, COUNTRY_REGION, className='w-50',
                                              id='country_dropdown_multiple', multi=True)

COUNTRY_SINGLE_INPUT = Input(country_dropdown.id, 'value')
START_DATE_INPUT = Input('date-picker', 'start_date')
END_DATE_INPUT = Input('date-picker', 'end_date')
WEEK_INPUT = Input('by-week', 'value')
DEFAULT_INPUTS = [COUNTRY_SINGLE_INPUT, START_DATE_INPUT, END_DATE_INPUT, WEEK_INPUT]


def date_value_callback(outputs, inputs=None):
    """
    This method creates a callback that is a form of default for graphs that take the default inputs defined above.
    :param outputs: the outputs the callback returns
    :param inputs: the inputs if you wish to override the default ones
    :return: the callback
    """
    if inputs is None:
        inputs = DEFAULT_INPUTS

    return app.callback([*outputs], [*inputs])


def convert_date_range(start_date, end_date):
    """
    Convert the string dates to datetime objects
    :param start_date: the start date
    :param end_date: the end date
    :return: the parsed datetime obejcts start and end
    """
    start_date_obj = datetime.datetime(year=2021, month=1, day=1, hour=0, minute=0, second=0)
    end_date_obj = datetime.datetime.today()
    end_date_obj = datetime.datetime.combine(end_date_obj, datetime.datetime.min.time())

    if start_date is not None:
        start_date_obj = datetime.datetime.fromisoformat(start_date)

    if end_date is not None:
        end_date_obj = datetime.datetime.fromisoformat(end_date)

    return start_date_obj, end_date_obj


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


@date_value_callback([Output('covid-cases', 'children'), Output('covid-deaths', 'children')])
def covid_cases_deaths(value, start_date, end_date, by_week):
    """
    Compares the cases and deaths of COVID-19
    :param value: the country value from the dropdown
    :param start_date: the start date for the data
    :param end_date: the end date for the data
    :param by_week: true to aggregate data by week or false by day
    :return: the output graph
    """
    date_field = WEEK if by_week else DATE_RECORDED
    date_title = 'Week' if by_week else 'Day'

    start_date, end_date = convert_date_range(start_date, end_date)

    if value is not None:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, value, start_date, end_date)

        if by_week:
            pu.convert_date_field_to_week(data, DATE_RECORDED, week_number=False)
            data = data.group_aggregate([COUNTRY_REGION, WEEK])

        graph_config_cases = du.GraphConfig() \
            .x(data[date_field]) \
            .y(data[NEW_CASES]) \
            .type('line') \
            .marker().color('#0074D9').proceed() \
            .layout().title(f'New Covid-19 Cases By {date_title}').xaxis(date_title).yaxis('New Cases').proceed()

        cases = du.create_graph(graph_config_cases)

        graph_config_deaths = du.GraphConfig() \
            .x(data[date_field]) \
            .y(data[DEATHS]) \
            .type('line') \
            .marker().color('#0074D9').proceed() \
            .layout().title(f'New Covid-19 Deaths By {date_title}').xaxis(date_title).yaxis('Deaths').proceed()

        deaths = graph_config_deaths.build()

        return [html.Div(
            dcc.Graph(id='line-chart',
                      figure=cases)
        ), html.Div(
            dcc.Graph(id='line-chart1',
                      figure=deaths)
        )]
    else:
        return html.Div(
            dcc.Graph()
        ), ''


@date_value_callback([Output('covid-cases-deaths', 'children')])
def covid_confirmed_death(value, start_date, end_date, by_week):
    date_field = WEEK if by_week else DATE_RECORDED
    date_title = 'Week' if by_week else 'Day'

    start_date, end_date = convert_date_range(start_date, end_date)

    if value:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, value,
                                        start_date, end_date)

        if by_week:
            pu.convert_date_field_to_week(data, DATE_RECORDED, week_number=False)
            data = data.group_aggregate([COUNTRY_REGION, WEEK])

        fig = make_subplots(1, 2)
        fig.add_trace(du.create_plotly_figure(data, 'bar', x=date_field, y=NEW_CASES, graph_object=True), row=1, col=1)
        fig.add_trace(du.create_plotly_figure(data, 'scatter', x=date_field, y=DEATHS, line=dict(color='red'), graph_object=True),
                      row=1, col=1)

        return [html.Div(
            dcc.Graph(
                id='covid-cases-deaths-graph',
                figure=fig
            )
        )]
    else:
        return ['']


class _CompareCasesOptions(Enum):
    NEW_CASES = 1
    CONFIRMED_CASES = 2
    NEW_DEATHS = 3
    DEATHS = 4


def _parse_case_options(date_type, cases_options):
    """
    Parses the cases options and returns the title, yaxis and column attribute
    :param date_type: the date type to display in the title
    :param cases_options: the option value
    :return: the title, yaxis and column attribute
    """
    if cases_options == _CompareCasesOptions.NEW_CASES.value:
        return f'New Covid-19 Cases By {date_type}', 'New Cases', NEW_CASES
    elif cases_options == _CompareCasesOptions.CONFIRMED_CASES.value:
        return f'Confirmed Covid-19 Cases By {date_type}', 'Confirmed Cases', CONFIRMED
    elif cases_options == _CompareCasesOptions.NEW_DEATHS.value:
        return f'New Covid-19 Deaths By {date_type}', 'New Deaths', NEW_DEATHS
    elif cases_options == _CompareCasesOptions.DEATHS.value:
        return f'Covid-19 Deaths By {date_type}', 'Deaths', DEATHS
    else:
        raise ValueError(f'Unknown option for _CompareCasesOptions enumeration: {cases_options}')


@date_value_callback([Output('compare-covid', 'children')], [Input(country_dropdown_multiple.id, 'value'),
                                                             Input('compare-cases-options', 'value'),
                                                             *DEFAULT_INPUTS[1:]])
def compare_country_cases(values, compare_cases_options, start_date, end_date, by_week):
    """
    Compares the cases of COVID-19 between multiple countries specified in the values list
    :param values: the country value from the dropdown
    :param compare_cases_options: the value for which attribute to compare
    :param start_date: the start date for the data
    :param end_date: the end date for the data
    :param by_week: true to aggregate data by week or false by day
    :return: the output graph
    """
    date_field = WEEK if by_week else DATE_RECORDED
    date_title = 'Week' if by_week else 'Day'
    title, yaxis, column = _parse_case_options(date_title, compare_cases_options)

    start_date, end_date = convert_date_range(start_date, end_date)

    if values:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, values,
                                        start_date, end_date, multi_value=True)

        if by_week:
            pu.convert_date_field_to_week(data, DATE_RECORDED, week_number=False)
            data = data.group_aggregate([COUNTRY_REGION, WEEK])

        graph = du.create_plotly_figure(data, 'line', x=date_field, y=column, color=COUNTRY_REGION,
                                        title=title,
                                        xaxis=date_title,
                                        yaxis=yaxis)

        return [html.Div(dcc.Graph(id='line-chart-compare', figure=graph))]
    else:
        return [html.Div(dcc.Graph())]


"""
TODO compare 2/3 countries (configurable) (multi-dropdown), compare with hover data per country basis etc.

Add more comments to this file
"""

app.layout = du.get_layout(const.LAYOUT_FILE, {
    'country_dropdown': country_dropdown,
    'country_dropdown_multiple': country_dropdown_multiple
})


# app.layout = html.Div([
#     html.Div(
#         [
#             html.Div([
#                 html.Div(country_dropdown, className='col'),
#                 html.Div(dcc.DatePickerRange(
#                     id='date-picker',
#                     min_date_allowed=datetime.date(2020, 1, 22),
#                     max_date_allowed=datetime.date.today(),
#                     end_date=datetime.date.today(),
#                     display_format='D/M/YYYY'
#                 ), className='col'),
#                 html.Div(dcc.Checklist(
#                     id='by-week',
#                     options=[
#                         {'label': 'Week', 'value': 'true'}
#                     ]
#                 ), className='col')
#             ], className='row form-group'),
#             html.Div([
#                 html.Div(id='covid-cases'),
#                 html.Div(id='covid-deaths')
#             ])
#         ], className='row align-items-center'
#     )
# ], className='container')


def main():
    """
    The main entrypoint into the program
    :return: None
    """
    import sys

    debug = '-d' in sys.argv
    app.run_server(debug=debug)


if __name__ == '__main__':
    main()
