import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
from dash.dependencies import Input, Output
from data_access import db_read
import os

app = dash.Dash(__name__)

# Define layout with dropdown and graph
app.layout = html.Div([
    html.H1("Weather Time Series Data"),
    dcc.Dropdown(
        id="country-selector",
        options=[],  # Will be dynamically populated
        placeholder="Select a country",
    ),
    dcc.Graph(id="time-series-plot"),
    dcc.Graph(id='air-pollution-plot')
])


# Callback to populate dropdown options dynamically
@app.callback(
    Output("country-selector", "options"),
    # Dummy input; can use something else for updates
    Input("country-selector", "value")
)
def populate_country_dropdown(_):
    db_url = os.environ.get('DB_URL')
    # Fetch unique country names from the database
    country_names = db_read.get_unique_countries(db_url)
    return [{"label": country, "value": country} for country in country_names]

# Callback to update graph based on selected country


@app.callback(
    Output("time-series-plot", "figure"),
    Input("country-selector", "value")  # Country selection as input
)
def update_graph(selected_country):
    db_url = os.environ.get('DB_URL')
    # Fetch filtered data based on the selected country
    df = db_read.read_weather_data(db_url, country_name=selected_country)

    # Create a plot for the selected country
    fig = px.line(
        df,
        x="date",
        y="value",
        color="measure",
        title=f"Weather Data Over Time for {selected_country}"
    )
    return fig


@app.callback(
    Output("air-pollution-plot", "figure"),
    Input("country-selector", "value")  # Country selection as input
)
def update_graph(selected_country):
    db_url = os.environ.get('DB_URL')
    # Fetch filtered data based on the selected country
    df = db_read.read_air_pollution_data(db_url, country_name=selected_country)
    df.drop_duplicates(
        subset=['country_name', 'date', 'measure'], inplace=True)
    # Create a plot for the selected country
    fig = px.line(
        df,
        x="date",
        y="value",
        color="measure",
        title=f"Air Pollution Data Over Time for {selected_country}"
    )
    return fig


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port=8050)
