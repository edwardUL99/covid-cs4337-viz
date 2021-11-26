import datetime
import sys

import dash
from dash import html
from dash import dcc
from dash.dependencies import Input, Output

import dataloader
import dashutils as du
import pandasutils as pu

from fields import *


app = dash.Dash(__name__,
                external_stylesheets=['https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css'])


def get_data():
    def drop_rep_ireland(df):
        df = df[df[COUNTRY_REGION] != 'Republic of Ireland']
        return df

    dataloader.add_processing_function(drop_rep_ireland)

    load_new_data = not dataloader.current_data_exists() or '-l' in sys.argv
    return dataloader.load_data(load_new_data)


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
    if inputs is None:
        inputs = DEFAULT_INPUTS

    return app.callback([*outputs], [*inputs])


def convert_date_range(start_date, end_date):
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
            'Please select a country',
            className='row mt-5 mb-5 text-center'
        ), ''


@date_value_callback([Output('compare-covid', 'children')], [Input(country_dropdown_multiple.id, 'value'),
                                                             *DEFAULT_INPUTS[1:]])
def compare_country_cases(values, start_date, end_date, by_week):
    date_field = WEEK if by_week else DATE_RECORDED
    date_title = 'Week' if by_week else 'Day'

    start_date, end_date = convert_date_range(start_date, end_date)

    if values:
        data = filter_by_value_and_date(df, COUNTRY_REGION, DATE_RECORDED, values,
                                        start_date, end_date, multi_value=True)

        if by_week:
            pu.convert_date_field_to_week(data, DATE_RECORDED, week_number=False)
            data = data.group_aggregate([COUNTRY_REGION, WEEK])

        graph = du.create_plotly_figure(data, 'line', x=date_field, y=NEW_CASES, color=COUNTRY_REGION,
                                        title=f'New Covid-19 Cases By {date_title}',
                                        xaxis={'text': date_title},
                                        yaxis={'text': 'New Cases'})

        return [html.Div(dcc.Graph(id='line-chart-compare', figure=graph))]
    else:
        return ['']


"""
TODO compare 2/3 countries (configurable) (multi-dropdown), compare with hover data per country basis etc.

Add more comments to this file
"""

app.layout = du.get_layout({
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
    import sys

    debug = '-d' in sys.argv
    app.run_server(debug=debug)


if __name__ == '__main__':
    main()
