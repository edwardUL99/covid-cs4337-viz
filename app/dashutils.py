"""
This class provides utility functions and classes for this project
"""
import os.path

from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

import base64

import const


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
        if 'id' not in kwargs:
            self.id = f'{column}-dropdown'
            kwargs['id'] = self.id

        if 'options' in kwargs:
            kwargs.pop('options')

        self.id = f'{column}-dropdown'

        kwargs['options'] = ColumnDropdown._create_options(df, column)

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
    """
    This class provides configuration functionality to pass into create_graph. It is used for creating dash dict
    graph objects.

    For plotly objects, see the create_plotly_figure function
    """
    class MarkerConfig:
        """
        This is config used by GraphConfig to configure the marker of the graph
        """
        def __init__(self, gc):
            """
            Initialise the MarkerConfig object
            :param gc: the GraphConfig it belongs to
            """
            self._gc = gc
            self._color = '#0074D9'

        def color(self, color):
            """
            Set the color of the marker
            :param color: the new marker color
            :return: an instance of self
            """
            self._color = color

            return self

        def proceed(self):
            """
            Set the GraphConfig marker object and return the GraphConfig object to proceed with graph configuration
            :return: the instance of the graph config that owns this object
            """
            self._gc._marker = self.build()
            return self._gc

        def build(self):
            """
            Build the marker dictionary
            :return: marker dictionary
            """
            marker = {'color': self._color}

            return marker

    class LayoutConfig:
        """
        This class provides functionality for configuring the layout of the graph being created by GraphConfig
        """
        def __init__(self, gc):
            """
            Initialise the LayoutConfig
            :param gc: the graph config that owns this LayoutConfig
            """
            self._gc = gc
            self._title = 'Dash Plot',
            self._xaxis = ''
            self._yaxis = ''

        def title(self, title):
            """
            Set the title of the graph
            :param title: the graph title
            :return: an instance of self
            """
            self._title = title

            return self

        def xaxis(self, xaxis):
            """
            Set the name of the graph's xaxis
            :param xaxis: the name of the xaxis
            :return: an instance of self
            """
            self._xaxis = xaxis

            return self

        def yaxis(self, yaxis):
            """
            Set the name of the graph's yaxis
            :param yaxis: the name of the yaxis
            :return: an instance of self
            """
            self._yaxis = yaxis

            return self

        def proceed(self):
            """
            Set the graph config's layout object and return the graph config instance to proceed with graph
            configuration
            :return: the graph config instance that owns this object
            """
            self._gc.layout = self.build()
            return self._gc

        def build(self):
            """
            Build the layout dictionary
            :return: the layout dictionary
            """
            layout = {
                'title': self._title,
                'xaxis': {'title': self._xaxis},
                'yaxis': {'title': self._yaxis}
            }

            return layout

    def __init__(self):
        """
        Initialise the graph config object
        """
        self._x = ''
        self._y = ''
        self._type = ''
        self._marker_config = self.MarkerConfig(self)
        self._marker = None
        self._layout_config = self.LayoutConfig(self)
        self._layout = None

    def x(self, x):
        """
        Set the data that is used on the x axis
        :param x: the x axis data
        :return: an instance of self
        """
        self._x = x

        return self

    def y(self, y):
        """
        Set the data that is used on the y axis
        :param y: the y axis data
        :return: an instance of self
        """
        self._y = y

        return self

    def type(self, type):
        """
        Set the type of the graph
        :param type: graph type
        :return: an instance of self
        """
        self._type = type

        return self

    def marker(self):
        """
        Configure the graph's marker
        :return: an instance of this GraphConfig's MarkerConfig. Call proceed() to return to graph config
        """
        return self._marker_config

    def layout(self):
        """
        Configure the graph's layout
        :return: an instance of this GraphConfig's LayoutConfig. Call proceed(0 to return to graph config
        """
        return self._layout_config

    def build(self):
        """
        Build the graph dictionary using the configured parameters
        :return: graph dictionary
        """
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


_graph_functions_px = {
    'line': px.line,
    'bar': px.bar,
    'scatter': px.scatter,
    'pie': px.pie
}


_graph_functions_go = {
    'line': go.Line,
    'bar': go.Bar,
    'scatter': go.Scatter,
    'pie': go.Pie
}


def _construct_figure(df, figure_func, x, y, graph_object, **kwargs):
    try:
        if graph_object:
            return figure_func(x=df[x], y=df[y], **kwargs)
        else:
            if x is not None and y is not None:
                return figure_func(df, x=x, y=y, **kwargs)
            else:
                return figure_func(df, **kwargs)
    except ValueError as e:
        # sometimes plotly is buggy and throws a random error, so try and construct the graph again
        return _construct_figure(df, figure_func, x, y, graph_object, **kwargs)


def create_plotly_figure(df, figure_type, x, y, title=None, color=None, graph_object=False, **kwargs):
    """
    Create the plotly figure from the provided arguments
    :param df: the dataframe to plot
    :param figure_type: the type of the plotly figure to create
    :param x: the name of the column to use for the x axis
    :param y: the name of the column to use for the y axis
    :param title: the title of the graph
    :param color: the color to differentiate multiple aspects of the data
    :param graph_object: true if to use graph_objects instead of plotly.express
    :param kwargs: any extra parameters to pass into plotly
    :return: the created plotly figure
    """
    graph_funcs = _graph_functions_px if not graph_object else _graph_functions_go

    if figure_type not in graph_funcs:
        raise ValueError(f'Invalid figure_type {figure_type}. {graph_funcs.keys()} supported')

    extra_args = {
        'x': x,
        'y': y
    }

    if color:
        extra_args['color'] = color

    new_kwargs = {**extra_args, **kwargs, 'graph_object': graph_object}

    fig = _construct_figure(df, graph_funcs[figure_type], title=title,  **new_kwargs)
    px.scatter()

    if hasattr(fig, 'update_layout'):
        fig.update_layout(paper_bgcolor='white', plot_bgcolor='white')
        fig.update_xaxes(gridcolor='#eee')
        fig.update_yaxes(gridcolor='#eee')

    return fig


def create_graph(gc: GraphConfig):
    """
    Creates the graph using the provided graph config
    :param gc: the graph config object
    :return: the graph as a dict
    """
    return gc.build()


_headers = {
    'H1': html.H1,
    'H2': html.H2,
    'H3': html.H3,
    'H4': html.H4,
    'H5': html.H5,
    'H6': html.H6
}


def create_header(header_text, size):
    """
    Creates a header surrounded by a row and with text centered. Size is 'H1' - 'H6'
    Exposed to the get_layout function by default
    :param header_text: the text for the header
    :param size: the header size, ranging from H1 to H6
    :return: the created header row
    """
    header = _headers.get(size.upper(), html.H1)

    return dbc.Row(
        header(header_text),
        className='text-center'
    )


def create_navigation_item(text, href):
    """
    Creates a dash bootstrap NavItem component with a navlink object pointing to the provided href and text.
    Exposed to the get_layout function by default
    :param text: the text to display on the link
    :param href: the reference for the link
    :return: the created navigation item
    """
    return dbc.NavItem(
        dbc.NavLink(text, href=href, external_link=True)
    )


def create_text_box(text, enclose_in_card=False):
    """
    Creates a box of vertically and horizontally centered text.
    Exposed to get_layout by default
    :param text: the text to display
    :param enclose_in_card: if true, enclose in a div with class card
    :return: the created element
    """
    class_name = 'd-flex align-items-center justify-content-center'

    if enclose_in_card:
        class_name += ' card mt-2 shadow'

    return html.Div(
        text,
        className=class_name,
        style={'height': '200px'}
    )


_allowed_functions = [create_header, create_navigation_item, create_text_box]


def _clean_variables(variables: dict):
    """
    Clean potentially dangerous variables from the variables dictionary
    :param variables: the dictionary of variables
    :return: the cleaned variables dictionary
    """
    dangerous_variables = ['os', 'shutil', 'pathlib']
    import os, shutil, pathlib
    dangerous_modules = [os, shutil, pathlib]
    # if any value is a string, don't evaluate it and also don't allow callable values
    variables = {k: v for k, v in variables.items() if k not in dangerous_variables
                 and (not callable(v) or v in _allowed_functions) and v not in dangerous_modules}

    return variables


def _get_covid_image():
    with open(os.path.join(const.FILE_DIR, 'app', 'assets', 'covid.png'), 'rb') as img:
        return f'data:image/png;base64,{base64.b64encode(img.read()).decode()}'


def get_layout(layout_file, variables: dict = None):
    """
    Creates and returns the layout for the app
    :param layout_file: the path to the layout file
    :param variables: variables in the layout file that are required.
    If the variables contains os, shutil, pathlib or any callable objects, they will be discarded
    :return: the app's layout
    """
    import datetime

    parameters = {
        'html': html,
        'dcc': dcc,
        'dbc': dbc,
        'datetime': datetime,
        'create_header': create_header,
        'create_navigation_item': create_navigation_item,
        'covid_nav_logo': _get_covid_image()
    }

    if variables is not None:
        parameters = {**parameters, **variables}

    parameters = _clean_variables(parameters)

    with open(layout_file, 'r') as f:
        layout = f.read()

    dash_layout = eval(layout, parameters)

    return dash_layout
