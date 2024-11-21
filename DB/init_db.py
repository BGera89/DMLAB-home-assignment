from sqlalchemy import create_engine, Column, Integer, Float, Date, MetaData, Table, String, DateTime, inspect
from sqlalchemy.exc import OperationalError
import pandas as pd


def initialize_database(db_url):
    """
    Ensure the database has the required table schema.
    :param db_url: Database connection URL.
    """

    metadata = MetaData()

    # Define the weather_data table schema
    daily_weather_data = Table(
        'daily_weather_data', metadata,
        Column('place_name', String),
        Column('date_id', DateTime, nullable=False),
        Column('temperature_2m_cels', Float),
        Column('rain_mm', Float),
        Column('wind_speed_kmh', Float)
    )

    air_quality_data = Table(
        'air_quality_data', metadata,
        Column('place_name', String),
        Column('date_id', DateTime, nullable=False),
        Column('pm10', Float),
        Column('pm2_5', Float),
        Column('carbon_dioxide', Float),
        Column('nitrogen_dioxide', Float),
        Column('sulphur_dioxide', Float),
        Column('ozone', Float)
    )

    forecast_weather_data = Table(
        'forecast_weather_data', metadata,
        Column('place_name', String),
        Column('date_id', DateTime, nullable=False),
        Column('temperature_2m_cels', Float),
        Column('rain_mm', Float),
        Column('wind_speed_kmh', Float)
    )

    places_data = Table(
        'places_data', metadata,
        Column('place_name', String),
        Column('longitude', Float),
        Column('latitude', Float),
    )

    try:
        # Create the database engine
        engine = create_engine(db_url)

        # Use the public API to check if the tables exist
        inspector = inspect(engine)

        # List of tables to check and create
        tables_to_create = [daily_weather_data,
                            air_quality_data,
                            forecast_weather_data,
                            places_data]

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


def dms_to_dd(dms):
    """
    Converts degree minute second (dms) to decimal degree (dd).

    :param dms: degree minute second in a string format.
    """
    degrees, minutes = map(float, dms.split(':'))
    return degrees + (minutes / 60)


def load_places_to_db(excel_path: str, db_url: str, table_name: str):
    """
    Loads the Hungarian places into the PostgreSQL database

    :param excel_path: path to excel file
    :param db_url: Database connection URL.
    :param table_name: Database table name.
    """
    # Read Excel file
    df = pd.read_excel(excel_path, usecols=[
                       'Helységnév', 'Keleti hosszúság, fok:perc.századperc', 'Északi szélesség, fok:perc.századperc'])

    # Rename columns
    df.columns = ['place_name', 'longitude_dms', 'latitude_dms']

    # Convert DMS to DD
    df['longitude'] = df['longitude_dms'].apply(dms_to_dd)
    df['latitude'] = df['latitude_dms'].apply(dms_to_dd)

    # Drop the original DMS columns
    df = df.drop(['longitude_dms', 'latitude_dms'], axis=1)

    # Connect to the database
    engine = create_engine(db_url)

    # Fetch existing data from the database
    existing_data_df = pd.read_sql_table(
        table_name, con=engine, columns=['place_name'])

    # Identify new records using a left merge (pandas equivalent of a SQL left outer join)
    new_records_df = pd.merge(
        df, existing_data_df, on='place_name', how='left', indicator=True)
    new_records_df = new_records_df[new_records_df['_merge']
                                    == 'left_only']
    new_records_df = new_records_df.drop('_merge', axis=1)

    # Insert new records in bulk into the database
    if not new_records_df.empty:
        new_records_df.to_sql(table_name, con=engine,
                              if_exists='append', index=False)
        print("New places loaded into the database successfully.")
    else:
        print("No new places to load into the database.")


if __name__ == "__main__":
    db_url = "postgresql://postgres:mysecretpassword@postgres:5432/postgres"
    initialize_database(db_url)
    excel_path = 'tables.helyseg_hu.xls'
    table_name = 'places_data'  # Replace with your actual table name
    load_places_to_db(excel_path, db_url, table_name)
