from dash.dependencies import Input, Output, State
from dash import dcc
import dash
import plotly.express as px
import os
from data_access import db_read
import sys
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
        html.Button("Fetch Weather", id="fetch-weather-btn"),
    ]),
    html.Div([], id="weather-output"),
    html.Div([], style={'height': '50px'})
])


@app.callback(
    Output("place-selector", "options"),
    [Input("weather-output", "children")]
)
def refresh_place_dropdown(n_clicks):
    # Fetch the latest place names from the database
    db_url = os.environ['DB_URL']
    place_names = db_read.get_unique_countries(db_url)
    place_names = db_read.get_unique_countries(db_url)
    # Return updated dropdown options
    return [{"label": place, "value": place} for place in place_names]


@ app.callback(
    Output("time-series-plot", "figure"),
    Input("place-selector", "value"),
    prevent_initial_call=True
)
def update_weather_graph(selected_place):
    db_url = os.environ['DB_URL']
    df = db_read.read_weather_data(db_url, place_name=selected_place)
    df.drop_duplicates(subset=['date_id', 'measure'], inplace=True)
    fig = px.line(
        df,
        x="date_id",
        y="value",
        color="measure",
        title=f"Weather Data Over Time for {selected_place}"
    )
    return fig


@ app.callback(
    Output("air-pollution-plot", "figure"),
    Input("place-selector", "value"),
    prevent_initial_call=True
)
def update_air_pollution_graph(selected_place):
    db_url = os.environ['DB_URL']
    df = db_read.read_air_pollution_data(db_url, place_name=selected_place)
    # df.drop_duplicates(
    #     subset=['place_name', 'date', 'measure'], inplace=True)
    fig = px.line(
        df,
        x="date_id",
        y="value",
        color="measure",
        title=f"Air Pollution Data Over Time for {selected_place}"
    )
    return fig


@app.callback(
    Output("weather-output", "children"),
    [Input("fetch-weather-btn", "n_clicks")],
    [State("place-name-selector", "value"),]
)
def fetch_weather(n_clicks, selected_place_name):
    if not n_clicks:
        return "Select a place and click 'Fetch Weather'."
    if selected_place_name is None:
        return "Please select a place."

    db_url = os.environ['DB_URL']
    api_url = os.environ['API_FETCHER_URL'] + '/weather'
    coords = db_read.get_coordinates_for_place_name(
        db_url, selected_place_name)
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
    db_url = os.environ['DB_URL']
    # Fetch unique place names from the database
    place_names = db_read.get_unique_place_names(db_url)
    return [{"label": place_name, "value": place_name} for place_name in place_names]
