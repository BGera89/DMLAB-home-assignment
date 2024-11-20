import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from data_access.data_write import save_to_postgres


class WeatherDataFetcher:
    """
    A class to handle API calls to Open-Meteo and process daily weather data.
    """

    def __init__(self, cache_path=".cache", cache_expiry=-1, retries=5, backoff_factor=0.2):
        """
        Initialize the WeatherDataFetcher with caching and retry mechanisms.
        :param cache_path: Path for caching API responses.
        :param cache_expiry: Expiration time for cache (default: no expiration).
        :param retries: Number of retries on request failures.
        :param backoff_factor: Factor for exponential backoff in retries.
        """
        self.session = self._setup_session(
            cache_path, cache_expiry, retries, backoff_factor)
        self.client = openmeteo_requests.Client(session=self.session)

    @staticmethod
    def _setup_session(cache_path, cache_expiry, retries, backoff_factor):
        """
        Set up a cached and retry-enabled session.
        :param cache_path: Path for caching API responses.
        :param cache_expiry: Expiration time for cache.
        :param retries: Number of retries on request failures.
        :param backoff_factor: Factor for exponential backoff in retries.
        :return: Configured requests session.
        """
        cache_session = requests_cache.CachedSession(
            cache_path, expire_after=cache_expiry)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

        return retry_session

    def fetch_daily_weather_data(self, latitude, longitude,
                                 start_date, end_date,
                                 timezone="Europe/Berlin"):
        """
        Fetch weather data for a specific location and time period.
        :param latitude: Latitude of the location.
        :param longitude: Longitude of the location.
        :param start_date: Start date for weather data.
        :param end_date: End date for weather data.
        :param daily_variables: List of daily variables to fetch.
        :param timezone: Timezone for data (default: Europe/Berlin).
        :return: Response object from the API.
        """

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ["temperature_2m_mean", "rain_sum",
                      "wind_speed_10m_max", "shortwave_radiation_sum"],
            "timezone": timezone,
        }
        responses = self.client.weather_api(url, params=params)
        return responses[0]  # Assuming single location response for now

    def fetch_forecast_weather_data(self, latitude, longitude,
                                    start_date, end_date,
                                    temporal_resolution='hourly_6',
                                    timezone="Europe/Berlin"):
        """
        Fetch weather data for a specific location and time period.
        :param latitude: Latitude of the location.
        :param longitude: Longitude of the location.
        :param start_date: Start date for weather data.
        :param end_date: End date for weather data.
        :param daily_variables: List of daily variables to fetch.
        :param timezone: Timezone for data (default: Europe/Berlin).
        :return: Response object from the API.
        """

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ["temperature_2m", "rain", "wind_speed_10m", "shortwave_radiation"],
            "temporal_resolution": temporal_resolution,
            "timezone": timezone,
        }
        responses = self.client.weather_api(url, params=params)
        return responses[0]  # Assuming single location response for now

    def fetch_air_quality_data(self, latitude, longitude,
                               start_date, end_date,
                               temporal_resolution='hourly_6',
                               timezone="Europe/Berlin"
                               ):
        """
        Fetch weather data for a specific location and time period.
        :param latitude: Latitude of the location.
        :param longitude: Longitude of the location.
        :param start_date: Start date for weather data.
        :param end_date: End date for weather data.
        :param hourly_variables: List of daily variables to fetch.
        :param timezone: Timezone for data (default: Europe/Berlin).
        :return: Response object from the API.
        """

        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": [
                "pm10", "pm2_5", "carbon_dioxide",
                "nitrogen_dioxide", "sulphur_dioxide",
                "ozone"],
            "temporal_resolution": temporal_resolution,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
        }
        responses = self.client.weather_api(url, params=params)
        return responses[0]


class WeatherDataProcessor:
    """
    Class that processes the Weather data returned by the WeatherDataFetcher class
    """

    def __init__(self, response):
        """
        Initialize the processor with the API response.
        :param response: API response object containing weather data.
        """
        self.response = response

    def process_daily_data(self):
        """
        Process daily weather data from the API response.
        :return: Pandas DataFrame of daily weather data.
        """
        daily_variables = ["temperature_2m_mean", "rain_sum",
                           "wind_speed_10m_max", "shortwave_radiation_sum"]

        daily = self.response.Daily()
        variables = [daily.Variables(i).ValuesAsNumpy()
                     for i in range(len(daily_variables))]

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            daily_data[daily_variables[idx]] = var

        return pd.DataFrame(data=daily_data)

    def process_forecast_weather_data(self):
        """
        Process daily weather data from the API response.
        :return: Pandas DataFrame of daily weather data.
        """
        hourly_variables = ["temperature_2m", "rain",
                            "wind_speed_10m", "shortwave_radiation"]
        hourly = self.response.Hourly()
        variables = [hourly.Variables(i).ValuesAsNumpy()
                     for i in range(len(hourly_variables))]

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            hourly_data[hourly_variables[idx]] = var

        return pd.DataFrame(data=hourly_data)

    def process_air_quality_data(self):
        hourly_variables = [
            "pm10", "pm2_5", "carbon_dioxide",
            "nitrogen_dioxide", "sulphur_dioxide",
            "ozone"]
        hourly = self.response.Hourly()
        variables = [hourly.Variables(i).ValuesAsNumpy()
                     for i in range(len(hourly_variables))]

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            hourly_data[hourly_variables[idx]] = var

        return pd.DataFrame(data=hourly_data)


if __name__ == "__main__":
    # Testing and Initialization purposes
    fetcher = WeatherDataFetcher()

    country = 'Budapest'

    # daily_weather = fetcher.fetch_daily_weather_data(
    #     latitude=47.50241297012739,
    #     longitude=19.04873812789789,
    #     start_date="2024-06-03",
    #     end_date="2024-11-17",)
    # processor = WeatherDataProcessor(response=daily_weather)

    # daily_dataframe = processor.process_daily_data()
    # daily_dataframe['country_name'] = country
    # daily_dataframe = daily_dataframe[['country_name',
    #                                    'date',
    #                                    'temperature_2m_mean',
    #                                    'rain_sum',
    #                                    'wind_speed_10m_max',
    #                                    'shortwave_radiation_sum']]
    # print(daily_dataframe)

    # db_url = "postgresql://postgres:mysecretpassword@localhost:5433/postgres"

    # hourly_air = fetcher.fetch(
    #     latitude=47.50241297012739,
    #     longitude=19.04873812789789,
    #     start_date="2024-06-03",
    #     end_date="2024-11-17",
    # )
    # processor = WeatherDataProcessor(response=hourly_air)

    # air_dataframe = processor.process()
    # air_dataframe['country_name'] = country
    # air_dataframe = air_dataframe[['country_name',
    #                                'date',
    #                                'pm10',
    #                                'pm2_5',
    #                                'carbon_dioxide',
    #                                'nitrogen_dioxide',
    #                                'sulphur_dioxide',
    #                                'ozone']]
    # print(air_dataframe)

    # save_to_postgres(air_dataframe, db_url, table_name='air_quality_data')

    db_url = "postgresql://postgres:mysecretpassword@localhost:5433/postgres"

    hourly_air = fetcher.fetch_forecast_weather_data(
        latitude=47.50241297012739,
        longitude=19.04873812789789,
        start_date="2024-06-03",
        end_date="2024-11-17",
    )
    processor = WeatherDataProcessor(response=hourly_air)

    air_dataframe = processor.process_forecast_weather_data()
    air_dataframe['country_name'] = country
    air_dataframe = air_dataframe[['country_name',
                                   'date',
                                   "temperature_2m",
                                   "rain",
                                   "wind_speed_10m",
                                   "shortwave_radiation"]]
    print(air_dataframe)

    save_to_postgres(air_dataframe, db_url, table_name='forecast_weather_data')
