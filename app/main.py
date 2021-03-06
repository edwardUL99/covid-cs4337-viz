import datetime
import os
import sys
import argparse

basedir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.join(basedir, '..'))

import dash
from dash import html
from dash import dcc
from dash.dependencies import Input, Output

import dashutils as du
from data import pandasutils as pu
from data.fields import *

import const
from const import enable_logging, log

from transformations import convert_daily_to_week, filter_by_value_and_date, \
    get_variants_data, get_variants_sums, compute_variant_proportions, compute_monthly_cases_deaths, \
    compute_testing_metrics, get_vaccination_percentage_and_boosters

from enum import Enum

enable_logging(__name__ == '__main__')

app = dash.Dash(__name__,
                external_stylesheets=['https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css'],
                title='COVID-19 Visualisation Dashboard',
                assets_folder=os.path.join(const.FILE_DIR, 'app', 'assets'))

parser = argparse.ArgumentParser(description='The server for providing interactive COVID-19 visualisations')
parser.add_argument('-f', '--file', default=const.DATA_FILE, required=False, help='The path to the data.csv file'
                                                                                  ' generated by data/loader.py')
parser.add_argument('-d', '--debug', default=False, required=False, help='Enable debugging', action='store_true')
args = parser.parse_args()

DATA_FILE = args.file


def get_data():
    """
    Loads the data in from data.csv. Run the script separately before starting the server
    :return: the loaded data frame and also a dataframe for easy cases/deaths comparison with vaccinations
    """
    if not os.path.isfile(DATA_FILE):
        print(f'{DATA_FILE} does not exist. Run data/loader.py before calling this script')
        exit(1)
    else:
        log(f'Loading data from {DATA_FILE} into a DataFrame')
        df = pu.from_csv(DATA_FILE, convert_datetime=DATE_RECORDED, low_memory=False)

        return df


log('Starting')
df = get_data()
variants_df = get_variants_data(df)

_default_country = 'Ireland'
_dropdown_style = 'w-50'

country_dropdown = du.ColumnDropdown(df, COUNTRY_REGION, value=_default_country, className=_dropdown_style)
country_dropdown1 = du.ColumnDropdown(df, COUNTRY_REGION, value=_default_country, className=_dropdown_style,
                                      id='country_dropdown1')
country_dropdown_multiple = du.ColumnDropdown(df, COUNTRY_REGION, id='country_dropdown_multiple', multi=True,
                                              value=[_default_country, 'United Kingdom'], className=_dropdown_style)
country_dropdown_multiple1 = du.ColumnDropdown(df, COUNTRY_REGION, id='country_dropdown_multiple1',
                                               multi=True, value=[_default_country, 'United Kingdom'],
                                               className=_dropdown_style)
variants_dropdown = du.ColumnDropdown(variants_df, COUNTRY_REGION, id='eu_dropdown',
                                      value=_default_country, className=_dropdown_style)

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
    :return: the parsed datetime objects start and end
    """
    start_date_obj = datetime.datetime(year=2021, month=1, day=1, hour=0, minute=0, second=0)
    end_date_obj = datetime.datetime.today()
    end_date_obj = datetime.datetime.combine(end_date_obj, datetime.datetime.min.time())

    if start_date is not None:
        start_date_obj = datetime.datetime.fromisoformat(start_date)

    if end_date is not None:
        end_date_obj = datetime.datetime.fromisoformat(end_date)

    return start_date_obj, end_date_obj


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
            data = convert_daily_to_week(data)

        graph_config_cases = du.GraphConfig() \
            .x(data[date_field]) \
            .y(data[NEW_CASES]) \
            .type('line') \
            .marker().color('#0074D9').proceed() \
            .layout().title(f'New Covid-19 Cases By {date_title}').xaxis(date_title).yaxis('New Cases').proceed()

        cases = du.create_graph(graph_config_cases)

        graph_config_deaths = du.GraphConfig() \
            .x(data[date_field]) \
            .y(data[NEW_DEATHS]) \
            .type('line') \
            .marker().color('red').proceed() \
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
        return du.create_text_box('Please select a country from the top-left dropdown'), ''


@date_value_callback([Output('covid-cases-monthly', 'children'),
                      Output('covid-deaths-monthly', 'children')], DEFAULT_INPUTS[:-1] +
                     [Input('by_thousand_cases_deaths', 'value')])
def covid_cases_deaths_monthly(value, start_date, end_date, by_thousand):
    """
    Display the cases and deaths by a monthly basis
    :param value: the country value
    :param start_date: the start date of the time range
    :param end_date: the end date of the time range
    :param by_thousand: to display by 1000 or not
    :return: the graph output
    """
    start_date, end_date = convert_date_range(start_date, end_date)
    title = 'per thousand by month' if by_thousand else '(New) by month'

    if value:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, value, start_date, end_date)
        data, fields = compute_monthly_cases_deaths(data, by_thousand)

        data_cases = data[data['Type'] == fields[0]]
        data_deaths = data[data['Type'] == fields[1]]

        graph_cases = du.create_plotly_figure(data_cases, 'bar', x='Month', y='Count',
                                              title=f'Average Cases {title}')

        graph_deaths = du.create_plotly_figure(data_deaths, 'bar', x='Month', y='Count',
                                               title=f'Average Deaths {title}')
        graph_deaths.update_traces(marker_color='red')

        return [
            html.Div(
                dcc.Graph(
                    id='cov-cases-month-graph',
                    figure=graph_cases
                )
            ),
            html.Div(
                dcc.Graph(
                    id='cov-deaths-month-graph',
                    figure=graph_deaths
                )
            )
        ]
    else:
        return ['', '']


class _CompareCasesOptions(Enum):
    NEW_CASES = 1
    CONFIRMED_CASES = 2
    NEW_DEATHS = 3
    DEATHS = 4


def _parse_case_options(date_type, cases_options, by_thousand):
    """
    Parses the cases options and returns the title, yaxis and column attribute
    :param date_type: the date type to display in the title
    :param cases_options: the option value
    :param by_thousand: compare by thousand if it makes sense
    :return: the title, yaxis and column attribute, and override by_week
    """
    if cases_options == _CompareCasesOptions.NEW_CASES.value:
        if by_thousand:
            return f'New Covid-19 Cases By Week per 100,000', 'New Cases', INCIDENT_RATE, True
        else:
            return f'New Covid-19 Cases By {date_type}', 'New Cases', NEW_CASES, False
    elif cases_options == _CompareCasesOptions.CONFIRMED_CASES.value:
        return f'Confirmed Covid-19 Cases By {date_type}', 'Confirmed Cases', CONFIRMED, False
    elif cases_options == _CompareCasesOptions.NEW_DEATHS.value:
        if by_thousand:
            return f'New Covid-19 Deaths By Week per 100,000', 'New Deaths', DEATH_RATE, True
        else:
            return f'New Covid-19 Deaths By {date_type}', 'New Deaths', NEW_DEATHS, False
    elif cases_options == _CompareCasesOptions.DEATHS.value:
        return f'Covid-19 Deaths By {date_type}', 'Deaths', DEATHS, False
    else:
        raise ValueError(f'Unknown option for _CompareCasesOptions enumeration: {cases_options}')


@date_value_callback([Output('compare-covid', 'children')], [Input(country_dropdown_multiple.id, 'value'),
                                                             Input('compare-cases-options', 'value'),
                                                             Input('by_thousand', 'value'),
                                                             *DEFAULT_INPUTS[1:]])
def compare_country_cases(values, compare_cases_options, by_thousand, start_date, end_date, by_week):
    """
    Compares the cases of COVID-19 between multiple countries specified in the values list
    :param values: the country value from the dropdown
    :param compare_cases_options: the value for which attribute to compare
    :param by_thousand: to display values by_thousand if it makes sense
    :param start_date: the start date for the data
    :param end_date: the end date for the data
    :param by_week: true to aggregate data by week or false by day
    :return: the output graph
    """
    date_field = WEEK if by_week else DATE_RECORDED
    date_title = 'Week' if by_week else 'Day'
    title, yaxis, column, override_by_week = _parse_case_options(date_title, compare_cases_options, by_thousand)

    if override_by_week:
        by_week = True
        date_field = WEEK
        date_title = 'Week'

    start_date, end_date = convert_date_range(start_date, end_date)

    if values:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, values,
                                        start_date, end_date, multi_value=True)

        if by_week:
            data = convert_daily_to_week(data)

        if override_by_week:
            data[column] = data[column].rolling(window=2).mean()

        graph = du.create_plotly_figure(data, 'line', x=date_field, y=column, color=COUNTRY_REGION,
                                        title=title,
                                        labels={
                                            date_field: date_title,
                                            column: yaxis
                                        })

        return [html.Div(dcc.Graph(id='line-chart-compare', figure=graph))]
    else:
        return [du.create_text_box('Please select at least one country from the top-left dropdown')]


@date_value_callback([Output('compare-vaccinations', 'children'),
                      Output('boosters-given', 'children')], [Input(country_dropdown_multiple1.id, 'value'),
                                                              *DEFAULT_INPUTS[1:-1]])
def compare_vaccinations(values, start_date, end_date):
    """
    Compares the vaccination percentage of multiple countries
    :param values: the countries to compare
    :param start_date: the start of the date range
    :param end_date: the end of the date range
    :return:
    """
    date_field = WEEK
    date_title = 'Week'
    title = 'Percentage of people vaccinated by week'

    start_date, end_date = convert_date_range(start_date, end_date)

    if values:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, values, start_date, end_date,
                                        multi_value=True)
        data, boosters = get_vaccination_percentage_and_boosters(data)

        graph_percentage = du.create_plotly_figure(data, 'line', x=date_field, y=PERCENTAGE_VACCINATED,
                                                   color=COUNTRY_REGION,
                                                   title=title,
                                                   labels={
                                                       date_field: date_title,
                                                       PERCENTAGE_VACCINATED: 'Percentage Vaccinated'
                                                   })

        graph_boosters = du.create_plotly_figure(boosters, 'line', x=date_field, y=TOTAL_BOOSTERS, color=COUNTRY_REGION,
                                                 title='Total boosters given per hundred by week',
                                                 labels={
                                                     date_field: date_title,
                                                     TOTAL_BOOSTERS: 'Boosters given'
                                                 })

        return [html.Div(dcc.Graph(id='vaccines-compare-percentage', figure=graph_percentage)),
                html.Div(dcc.Graph(id='vaccines-compare-boosters', figure=graph_boosters))]
    else:
        return [du.create_text_box('Please select at least one country from the top-left dropdown'), '']


@date_value_callback([Output('country-testing-daily', 'children')], [Input(country_dropdown1.id, 'value'),
                                                                     *DEFAULT_INPUTS[1:-1]])
def compare_testing(value, start_date, end_date):
    """
    Compares a country's testing efforts
    :param value: the country value
    :param start_date: the start date of the time period
    :param end_date: the end date of the time period
    :return: the outputs
    """
    start_date, end_date = convert_date_range(start_date, end_date)

    if value:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, value, start_date, end_date)
        data = compute_testing_metrics(data)

        graph = du.create_plotly_figure(data, 'line', x=WEEK, y='Count', color='Type',
                                        title='Daily tests taken by week',
                                        labels={
                                            DATE_RECORDED: 'Day',
                                            POSITIVE_RATE: 'Positive Rate'
                                        },
                                        hover_data=[POSITIVE_RATE])

        return [
            html.Div(
                dcc.Graph(
                    id='covid-testing-graph',
                    figure=graph
                )
            )
        ]
    else:
        return [du.create_text_box('Please select a country from the top-left dropdown')]


@date_value_callback([Output('compare-variants', 'children'),
                      Output('variant-proportions', 'children')], [Input(variants_dropdown.id, 'value'),
                                                                   *DEFAULT_INPUTS[1:-1]])
def compare_variants(value, start_date, end_date):
    """
    Displays the proportions of variants
    :param value: the value of the country name
    :param start_date: the start date of the data range
    :param end_date: the end date of the data range
    :return: the output graph
    """
    start_date, end_date = convert_date_range(start_date, end_date)

    if value:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, value, start_date, end_date)
        variations_sum_data = get_variants_sums(data.copy())
        variations_proportions_data = compute_variant_proportions(data.copy())

        sum_graph = du.create_plotly_figure(variations_sum_data, 'line', x=DATE_RECORDED, y=NUMBER_DETECTIONS_VARIANT,
                                            color=VARIANT, title='Trend of variant detections over time',
                                            labels={
                                                DATE_RECORDED: 'Week',
                                                NUMBER_DETECTIONS_VARIANT: 'Number of Detections',
                                                VARIANT: 'Variant'
                                            })
        proportions_graph = du.create_plotly_figure(variations_proportions_data, 'pie', x=None, y=None,
                                                    values=NUMBER_DETECTIONS_VARIANT, names=VARIANT,
                                                    title='Proportion of variants detected',
                                                    labels={
                                                        VARIANT: 'Variant',
                                                        NUMBER_DETECTIONS_VARIANT: 'Number of Detections'
                                                    })

        return [
            html.Div(
                dcc.Graph(
                    id='compare-variants-graph',
                    figure=sum_graph
                )
            ),
            html.Div(
                dcc.Graph(
                    id='variant-proportions-graph',
                    figure=proportions_graph
                )
            )
        ]
    else:
        return [du.create_text_box('Please select a country from the top-left dropdown'), '']


app.layout = du.get_layout(const.LAYOUT_FILE, {
    'country_dropdown': country_dropdown,
    'country_dropdown1': country_dropdown1,
    'country_dropdown_multiple': country_dropdown_multiple,
    'country_dropdown_multiple1': country_dropdown_multiple1,
    'variants_dropdown': variants_dropdown
})


def main():
    """
    The main entrypoint into the program
    :return: None
    """
    app.run_server(debug=args.debug)


if __name__ == '__main__':
    main()
