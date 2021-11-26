"""
This class provides utility functions and classes for this project
"""
from dash import dcc, html


class ColumnDropdown(dcc.Dropdown):
    """
    A class that is a subclass of the dropdown class that is initialised from the provided column of the dataframe
    dataframe. The id field can be accessed using ColumnDropdown.id
    """
    def __init__(self, df, column, **kwargs):
        """
        Initialise the CountryDropdown
        :param df: the dataframe which is expected to have Country/Region in it to populate the dropdown
        :param column: the name of the column. It is expected to be in the dataframe
        :param kwargs: any arguments to pass up the hierarchy chain
        """
        if 'id' in kwargs:
            kwargs.pop('id')

        if 'options' in kwargs:
            kwargs.pop('options')

        self.id = f'{column}-dropdown'

        kwargs['options'] = ColumnDropdown._create_options(df, column)
        kwargs['id'] = self.id

        self.df = df
        self.column = column
        self.options = kwargs['options']

        super().__init__(**kwargs)

    @staticmethod
    def _create_options(df, column):
        """
        Create the options to fill this ColumnDropdown
        :param df: the dataframe to populate the options with
        :param column: the column to retrieve
        :return: the list of options
        """
        df = df[column].dropna()
        items = set(df.to_list())
        options = []

        for item in items:
            options.append({
                'label': item,
                'value': item
            })

        return options


class GraphConfig:
    class MarkerConfig:
        def __init__(self, gc):
            self._gc = gc
            self._color = '#0074D9'

        def color(self, color):
            self._color = color

            return self

        def proceed(self):
            self._gc._marker = self.build()
            return self._gc

        def build(self):
            marker = {'color': self._color}

            return marker

    class LayoutConfig:
        def __init__(self, gc):
            self._gc = gc
            self._title = 'Dash Plot',
            self._xaxis = ''
            self._yaxis = ''

        def title(self, title):
            self._title = title

            return self

        def xaxis(self, xaxis):
            self._xaxis = xaxis

            return self

        def yaxis(self, yaxis):
            self._yaxis = yaxis

            return self

        def proceed(self):
            self._gc.layout = self.build()
            return self._gc

        def build(self):
            layout = {
                'title': self._title,
                'xaxis': {'title': self._xaxis},
                'yaxis': {'title': self._yaxis}
            }

            return layout
    """
    This class provides configuration functionality to pass into create_graph
    """
    def __init__(self):
        self._x = ''
        self._y = ''
        self._type = ''
        self._marker_config = self.MarkerConfig(self)
        self._marker = None
        self._layout_config = self.LayoutConfig(self)
        self._layout = None

    def x(self, x):
        self._x = x

        return self

    def y(self, y):
        self._y = y

        return self

    def type(self, type):
        self._type = type

        return self

    def marker(self):
        return self._marker_config

    def layout(self):
        return self._layout_config

    def build(self):
        if self._marker is None:
            self._marker = self._marker_config.build()

        if self._layout is None:
            self._layout = self._layout_config.build()

        return {
            'data': [
                {
                    'x': self._x,
                    'y': self._y,
                    'type': self._type,
                    'marker': self._marker
                }
            ],
            'layout': self._layout
        }


def create_graph(gc: GraphConfig):
    return gc.build()


def get_layout(variables: dict = None):
    """
    Creates and returns the layout for the app
    :param variables: variables in the layout file that are required
    :return: the app's layout
    """
    import datetime

    parameters = {
        'html': html,
        'dcc': dcc,
        'datetime': datetime
    }

    if variables is not None:
        parameters = {**parameters, **variables}

    with open('layout.txt', 'r') as f:
        layout = f.read()

    dash_layout = eval(layout, parameters)

    return dash_layout
