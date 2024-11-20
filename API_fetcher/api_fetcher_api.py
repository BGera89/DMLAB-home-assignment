from fastapi import FastAPI, Query, HTTPException
import pandas as pd
from sqlalchemy import create_engine
from api_fetcher import WeatherDataFetcher  # Import your classes
from api_fetcher import WeatherDataProcessor
from data_access.data_write import save_to_postgres
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
    country_name: str = Query(...),
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
        # Instantiate the WeatherDataFetcher
        fetcher = WeatherDataFetcher()

        # Fetch the weather data
        response = fetcher.fetch_daily_weather_data(
            latitude=lat,
            longitude=lon,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone
        )

        # Process the weather data
        processor = WeatherDataProcessor(response)
        processed_data = processor.process_daily_data()

        processed_data['country_name'] = country_name
        processed_data = processed_data[['country_name',
                                         'date',
                                         'temperature_2m_mean',
                                         'rain_sum',
                                         'wind_speed_10m_max',
                                         'shortwave_radiation_sum']]
        processed_data.rename(columns={'date': 'date_id'}, inplace=True)

        # Save to the PostgreSQL database
        save_to_postgres(processed_data, DB_URL, TABLE_NAME)

        return {"message": "Weather data successfully saved to the database."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)
