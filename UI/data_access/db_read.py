import pandas as pd
from sqlalchemy import create_engine


def read_weather_data(connection_url, country_name):

    engine = create_engine(connection_url)

    query = f"""
        select a.country_name, a.date_id, t.*
        from daily_weather_data as a
        cross join lateral (
        values (a.temperature_2m_mean, 'tempreture_2m_mean'),
            (a.rain_sum, 'rain_sum'),
            (a.Wind_speed_10m_max, 'wind_speed_10m_max'),
            (a.Shortwave_radiation_sum, 'short_wave_radiation_sum')
        )
		as t (Value, measure)
        where a.country_name='{country_name}'
	order by 1,2
    """
    return pd.read_sql(query, engine)


def read_air_pollution_data(connection_url, country_name):

    engine = create_engine(connection_url)

    query = f"""
       select a.country_name, a.date_id, t.*
        from air_quality_data as a
        cross join lateral
        (
        values (a.pm10, 'pm10'),
                (a.pm2_5, 'pm2_5'),
                (a.carbon_dioxide , 'carbon_dioxide'),
                (a.nitrogen_dioxide , 'nitrogen_dioxide'),
                (a.sulphur_dioxide , 'sulphur_dioxide'),
                (a.ozone , 'ozone')
        ) as t (Value, measure)
        where a.country_name='{country_name}'
        order by 1,2
    """
    return pd.read_sql(query, engine)


def get_unique_countries(db_url):
    """
    Fetch the list of unique country names from the database.
    """
    engine = create_engine(db_url)
    query = "SELECT DISTINCT country_name FROM daily_weather_data"
    df = pd.read_sql(query, engine)

    return df['country_name'].values


def get_unique_place_names(db_url):
    engine = create_engine(db_url)
    query = f"SELECT DISTINCT place_name FROM places_data"
    df = pd.read_sql(query, engine)
    return df['place_name'].values


def get_coordinates_for_place_name(db_url, place_name):
    engine = create_engine(db_url)
    query = f"SELECT latitude, longitude FROM places_data WHERE place_name = '{place_name}'"
    df = pd.read_sql(query, engine)
    return df
