from dash.dependencies import Input, Output
from dash import dcc
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
        id="country-selector",
        options=[],  # Will be dynamically populated
        placeholder="Select a country",
    ),
    dcc.Graph(id="time-series-plot"),
    dcc.Graph(id='air-pollution-plot'),
    html.H1("Weather Information"),
    html.Div([
        dcc.Input(id="input-lat", type="number",
                  placeholder="Latitude", step=0.01),
        dcc.Input(id="input-lon", type="number",
                  placeholder="Longitude", step=0.01),
        html.Button("Fetch Weather", id="fetch-weather-btn"),
    ]),
    html.Div(id="weather-output")
])


@app.callback(
    Output("country-selector", "options"),
    Input("country-selector", "value")
)
def populate_country_dropdown(_):
    db_url = os.environ['DB_URL']
    # Fetch unique country names from the database
    country_names = db_read.get_unique_countries(db_url)
    return [{"label": country, "value": country} for country in country_names]


@app.callback(
    Output("time-series-plot", "figure"),
    Input("country-selector", "value")
)
def update_weather_graph(selected_country):
    db_url = os.environ['DB_URL']
    df = db_read.read_weather_data(db_url, country_name=selected_country)
    fig = px.line(
        df,
        x="date",
        y="value",
        color="measure",
        title=f"Weather Data Over Time for {selected_country}"
    )
    return fig


@ app.callback(
    Output("air-pollution-plot", "figure"),
    Input("country-selector", "value")
)
def update_air_pollution_graph(selected_country):
    db_url = os.environ['DB_URL']
    df = db_read.read_air_pollution_data(db_url, country_name=selected_country)
    df.drop_duplicates(
        subset=['country_name', 'date', 'measure'], inplace=True)
    fig = px.line(
        df,
        x="date",
        y="value",
        color="measure",
        title=f"Air Pollution Data Over Time for {selected_country}"
    )
    return fig


@app.callback(
    Output("weather-output", "children"),
    Input("fetch-weather-btn", "n_clicks"),
    [Input("input-lat", "value"), Input("input-lon", "value")]
)
def fetch_weather(n_clicks, lat, lon):
    if not n_clicks:
        return "Enter coordinates and click 'Fetch Weather'."
    if lat is None or lon is None:
        return "Please enter both latitude and longitude."

    try:
        # Use the service name defined in docker-compose.yml for inter-container communication
        response = requests.get(
            f"http://api-fetcher-service:5000/weather?lat={lat}&lon={lon}"
        )
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        return f"Weather Data: {data}"
    except requests.RequestException as e:
        return f"Error fetching weather data: {str(e)}"
