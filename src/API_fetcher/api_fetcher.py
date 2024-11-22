import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


class WeatherDataFetcher:
    """
    A class to handle API calls to Open-Meteo and process daily weather data.
    """

    def __init__(self, cache_path: str = ".cache",
                 cache_expiry: int = -1, retries: int = 5, backoff_factor: float = 0.2):
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
    def _setup_session(cache_path: str, cache_expiry: int, retries: int = 5, backoff_factor: float = 0.2):
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
        retry_session = retry(cache_session, retries, backoff_factor=0.2)

        return retry_session

    def fetch_daily_weather_data(self, latitude: float, longitude: float,
                                 start_date: str, end_date: str,
                                 timezone: str = "Europe/Berlin"):
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

    def fetch_forecast_weather_data(self, latitude: float, longitude: float,
                                    start_date: str, end_date: str,
                                    temporal_resolution: str = 'hourly_6',
                                    timezone: str = "Europe/Berlin"):
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

    def fetch_air_quality_data(self, latitude: float, longitude: float,
                               start_date: str, end_date: str,
                               temporal_resolution: str = 'hourly_6',
                               timezone: str = "Europe/Berlin"
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
    In the functions variables are defined inside the function 
    becaouse they are static for database use
    """

    def __init__(self, response, place_name: str):
        """
        Initialize the processor with the API response.
        :param response: API response object containing weather data.
        """
        self.response = response
        self.place_name = place_name

    def process_daily_data(self) -> pd.DataFrame:
        """
        Process daily weather data from the API response.
        :return: Pandas DataFrame of daily weather data.
        """
        daily_variables = ["temperature_2m_cels", "rain_mm",
                           "wind_speed_kmh"]

        daily = self.response.Daily()
        variables = [daily.Variables(i).ValuesAsNumpy()
                     for i in range(len(daily_variables))]

        daily_data = {
            "date_id": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            daily_data[daily_variables[idx]] = var

        daily_dataframe = pd.DataFrame(data=daily_data)

        daily_dataframe['place_name'] = self.place_name
        daily_dataframe = daily_dataframe[['place_name',
                                           'date_id',
                                           "temperature_2m_cels",
                                           "rain_mm",
                                           "wind_speed_kmh"
                                           ]]

        return daily_dataframe

    def process_forecast_weather_data(self) -> pd.DataFrame:
        """
        Process daily weather data from the API response.
        :return: Pandas DataFrame of daily weather data.
        """
        hourly_variables = ["temperature_2m_cels", "rain_mm",
                            "wind_speed_kmh"]
        hourly = self.response.Hourly()
        variables = [hourly.Variables(i).ValuesAsNumpy()
                     for i in range(len(hourly_variables))]

        hourly_data = {
            "date_id": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            hourly_data[hourly_variables[idx]] = var

        forecast_dataframe = pd.DataFrame(data=hourly_data)

        forecast_dataframe['place_name'] = self.place_name
        forecast_dataframe = forecast_dataframe[['place_name',
                                                 'date_id',
                                                 "temperature_2m_cels",
                                                 "rain_mm",
                                                 "wind_speed_kmh"
                                                 ]]
        return forecast_dataframe

    def process_air_quality_data(self) -> pd.DataFrame:
        hourly_variables = [
            "pm10", "pm2_5", "carbon_dioxide",
            "nitrogen_dioxide", "sulphur_dioxide",
            "ozone"]
        hourly = self.response.Hourly()
        variables = [hourly.Variables(i).ValuesAsNumpy()
                     for i in range(len(hourly_variables))]

        hourly_data = {
            "date_id": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="right",
            )
        }

        for idx, var in enumerate(variables):
            # Use variable names dynamically
            hourly_data[hourly_variables[idx]] = var

        air_q_dataframe = pd.DataFrame(data=hourly_data)

        air_q_dataframe['place_name'] = self.place_name
        air_q_dataframe = air_q_dataframe[['place_name',
                                           'date_id',
                                           'pm10',
                                           'pm2_5',
                                           'carbon_dioxide',
                                           'nitrogen_dioxide',
                                           'sulphur_dioxide',
                                           'ozone']]

        return air_q_dataframe
