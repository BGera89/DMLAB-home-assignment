from sqlalchemy import create_engine, Column, Integer, Float, Date, MetaData, Table, String
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect


def initialize_database(db_url):
    """
    Ensure the database has the required table schema.
    :param db_url: Database connection URL.
    """

    metadata = MetaData()

    # Define the weather_data table schema
    daily_weather_data = Table(
        'daily_weather_data', metadata,
        Column('country_name', String),
        Column('date', Date, nullable=False),
        Column('temperature_2m_mean', Float),
        Column('rain_sum', Float),
        Column('wind_speed_10m_max', Float),
        Column('shortwave_radiation_sum', Float)
    )

    air_quality_data = Table(
        'air_quality_data', metadata,
        Column('country_name', String),
        Column('date', Date, nullable=False),
        Column('pm10', Float),
        Column('pm2_5', Float),
        Column('carbon_dioxide', Float),
        Column('nitrogen_dioxide', Float),
        Column('sulphur_dioxide', Float),
        Column('ozone', Float)
    )

    forecast_weather_data = Table(
        'forecast_weather_data', metadata,
        Column('country_name', String),
        Column('date', Date, nullable=False),
        Column('temperature_2m', Float),
        Column('rain', Float),
        Column('wind_speed_10m', Float),
        Column('shortwave_radiation', Float)
    )

    try:
        # Create the database engine
        engine = create_engine(db_url)

        # Use the public API to check if the tables exist
        inspector = inspect(engine)

        # List of tables to check and create
        tables_to_create = [daily_weather_data,
                            air_quality_data,
                            forecast_weather_data]

        # Loop through each table and check if it exists
        for table in tables_to_create:
            if not inspector.has_table(table.name):
                print(
                    f"Table '{table.name}' does not exist, creating table...")
                # Create the specific table if it doesn't exist
                metadata.create_all(engine, tables=[table])
                print(f"Table '{table.name}' created.")
            else:
                print(
                    f"Table '{table.name}' already exists, no initialization needed.")

    except OperationalError as e:
        print(f"Error initializing the database: {e}")


if __name__ == "__main__":
    db_url = "postgresql://postgres:mysecretpassword@postgres:5432/postgres"
    initialize_database(db_url)
