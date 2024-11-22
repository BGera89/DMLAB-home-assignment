import datetime
import os
from utility import fetch_and_process_multiple
from api_fetcher import WeatherDataFetcher, WeatherDataProcessor

if __name__ == "__main__":
    # Testing and Initialization purposes
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")

    future_date = today + datetime.timedelta(days=7)
    future_date_str = future_date.strftime("%Y-%m-%d")

    past_3_days = today - datetime.timedelta(days=3)
    past_3_days_str = past_3_days.strftime("%Y-%m-%d")
    lat = 47.50241297012739
    lon = 19.04873812789789
    place_name = 'Budapest'
    timezone = "Europe/Berlin"

    fetch_process_pairs = [
        ("fetch_daily_weather_data", "process_daily_data",
         'daily_weather_data', "2024-06-03", past_3_days_str),
        ("fetch_air_quality_data", "process_air_quality_data",
         'air_quality_data', "2024-06-03", today),
        ("fetch_forecast_weather_data", "process_forecast_weather_data",
         'forecast_weather_data', past_3_days_str, future_date),
    ]
    # Instantiate the WeatherDataFetcher
    fetcher = WeatherDataFetcher()
    processor_class = WeatherDataProcessor

    for fetch_method, process_method, \
            table_name, start_timestamp, end_timestamp in fetch_process_pairs:
        fetch_and_process_multiple(
            fetcher=fetcher,
            processor_class=processor_class,
            fetch_method=fetch_method,
            process_method=process_method,
            latitude=lat,
            longitude=lon,
            start_date=start_timestamp,
            end_date=end_timestamp,
            place_name=place_name,
            timezone=timezone,
            connection_url=os.environ['DB_URL'], table_name=table_name)
