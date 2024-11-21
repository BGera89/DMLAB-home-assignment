from fastapi import FastAPI, Query, HTTPException
import pandas as pd
from sqlalchemy import create_engine
from api_fetcher import WeatherDataFetcher  # Import your classes
from api_fetcher import WeatherDataProcessor
from data_access.data_write import save_to_postgres
from utility import fetch_and_process_multiple
import os

app = FastAPI()

# Database configuration
#
DB_URL = os.environ['DB_URL']
TABLE_NAME = "daily_weather_data"


@app.get("/weather")
async def fetch_and_save_weather(
    lat: float = Query(...),
    lon: float = Query(...),
    place_name: str = Query(...),
    start_date: str = Query(default="2024-06-03"),
    end_date: str = Query(default="2024-11-17"),
    timezone: str = Query(default="Europe/Berlin")
):
    """
    Fetch, process, and save weather data to the database.
    - lat: Latitude of the location
    - lon: Longitude of the location
    - start_date: Start date for weather data (YYYY-MM-DD)
    - end_date: End date for weather data (YYYY-MM-DD)
    - timezone: Timezone for the weather data (default: Europe/Berlin)
    """
    try:

        fetch_process_pairs = [
            ("fetch_daily_weather_data", "process_daily_data",
             'daily_weather_data'),
            ("fetch_air_quality_data", "process_air_quality_data",
             'air_quality_data'),
            ("fetch_forecast_weather_data",
             "process_forecast_weather_data", 'forecast_weather_data'),
        ]
        # Instantiate the WeatherDataFetcher
        fetcher = WeatherDataFetcher()
        processor_class = WeatherDataProcessor

        for fetch_method, process_method, table_name in fetch_process_pairs:
            fetch_and_process_multiple(
                fetcher=fetcher,
                processor_class=processor_class,
                fetch_method=fetch_method,
                process_method=process_method,
                latitude=lat,
                longitude=lon,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
                place_name=place_name,
                db_url=os.environ['DB_URL'], table_name=table_name)

        return {"message": "Weather data successfully saved to the database."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)
