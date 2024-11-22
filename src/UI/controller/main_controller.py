from dash.dependencies import Input, Output, State
from dash import dcc
import dash
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import os
from data_access import data_read
import sys
import dash_bootstrap_components as dbc
from dash import dcc, html
from app_init import app
import requests

app_layout = html.Div([
    html.H1("Weather Time Series Data"),
    dcc.Dropdown(
        id="place-selector",
        options=[],  # Will be dynamically populated
        placeholder="Select a place",
    ),
    dcc.Graph(id="time-series-plot"),
    dcc.Graph(id='air-pollution-plot'),
    html.H1("Weather Information"),
    html.Div([
        dcc.Dropdown(
            id="place-name-selector",
            options=[],  # Will be dynamically populated
            placeholder="Select a place",
            searchable=True,
            value=None  # Allow the user to type and search
        ),
        dbc.Button("Fetch Weather", id="fetch-weather-btn",
                   style={
                       'backgroundColor': '#007bff',
                       'color': 'white',
                       'padding': '10px 20px',
                       'border': 'none',
                       'borderRadius': '5px',
                       'cursor': 'pointer'
                   }),
    ]),
    html.Div([], id="weather-output"),
    html.Div([], style={'height': '50px'})
])


@app.callback(
    Output("place-selector", "options"),
    [Input("weather-output", "children")]
)
def refresh_place_dropdown(n_clicks):
    """
    Updates the dropdown options for place selection.

    :param n_clicks: Number of clicks on the weather-output element.
    :return: A list of dictionaries containing label-value pairs for dropdown options.
    """
    connection_url = os.environ['DB_URL']
    place_names = data_read.get_unique_place_names_with_data(connection_url)
    # Return updated dropdown options
    return [{"label": place, "value": place} for place in place_names]


@app.callback(
    Output("time-series-plot", "figure"),
    Input("place-selector", "value"),
    prevent_initial_call=True
)
def update_weather_graph(selected_place):
    """
    Updates the weather graph based on the selected place.

    :param selected_place: The selected place name.
    :return: A line plot showing weather data over time.
    """
    connection_url = os.environ['DB_URL']
    df = data_read.read_weather_data(connection_url, place_name=selected_place)
    fig = px.line(
        df,
        x="date_id",
        y="value",
        color="measure",
        title=f"Weather Data Over Time for {selected_place}"
    )
    fig.update_layout(
        plot_bgcolor='white'
    )

    fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey'
    )

    return fig


@app.callback(
    Output("air-pollution-plot", "figure"),
    Input("place-selector", "value"),
    prevent_initial_call=True
)
def update_air_pollution_graph(selected_place):
    """
    Updates the air pollution graph for the selected place.

    :param selected_place: The selected place name.
    :return: A line plot with secondary y-axis showing air pollution data.
    """
    connection_url = os.environ['DB_URL']
    df = data_read.read_air_pollution_data(
        connection_url, place_name=selected_place)

    # Creating the figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Adding data for each measure
    for measure in df['measure'].unique():
        subset = df[df['measure'] == measure]
        if measure == "carbon_dioxide":
            fig.add_trace(
                go.Scatter(
                    x=subset["date_id"],
                    y=subset["value"],
                    name=measure
                ),
                secondary_y=True
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=subset["date_id"],
                    y=subset["value"],
                    name=measure
                ),
                secondary_y=False
            )

    # Updating layout
    fig.update_layout(
        title=f"Air Pollution Data Over Time for {selected_place}",
        xaxis_title="Date",
        yaxis_title="Primary Measures (μg/m^3)",
        yaxis2_title="Carbon Dioxide (ppm)",
        plot_bgcolor='white'
    )

    fig.update_xaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey'
    )
    fig.update_yaxes(
        mirror=True,
        ticks='outside',
        showline=True,
        linecolor='black',
        gridcolor='lightgrey'
    )
    return fig


@app.callback(
    Output("weather-output", "children"),
    [Input("fetch-weather-btn", "n_clicks")],
    [State("place-name-selector", "value")]
)
def fetch_weather(n_clicks, selected_place_name):
    """
    Fetches weather data for the selected place and displays it.

    :param n_clicks: Number of times the 'Fetch Weather' button has been clicked.
    :param selected_place_name: The name of the selected place.
    :return: A message indicating success or failure of the data fetch operation.
    """
    if not n_clicks:
        return "Select a place and click 'Fetch Weather'."
    if selected_place_name is None:
        return "Please select a place."

    connection_url = os.environ['DB_URL']
    api_url = os.environ['API_FETCHER_URL'] + '/weather'
    coords = data_read.get_coordinates_for_place_name(
        connection_url, selected_place_name)
    if coords.empty:
        return "Coordinates not found for the selected place."

    lat, lon = float(coords['latitude'].values[0]), float(
        coords['longitude'].values[0])

    try:
        # Use the service name defined in docker-compose.yml for inter-container communication
        response = requests.get(
            api_url, params={'lat': lat,
                             'lon': lon,
                             "place_name": selected_place_name})
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        return f"Weather Data: {data}"
    except requests.RequestException as e:
        return f"Error fetching weather data: {str(e)}"


@app.callback(
    Output("place-name-selector", "options"),
    [Input("place-name-selector", "search_value")],
    prevent_initial_call=False  # Prevents callback from running on app load
)
def populate_place_name_selector(search_value):
    """
    Populates the place name selector with options fetched from the database.

    :param search_value: The current search value entered in the selector.
    :return: A list of dictionaries containing label-value pairs for dropdown options.
    """
    connection_url = os.environ['DB_URL']
    # Fetch unique place names from the database
    place_names = data_read.get_unique_place_names(connection_url)
    return [{"label": place_name, "value": place_name} for place_name in place_names]
