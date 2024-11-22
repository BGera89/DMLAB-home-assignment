from sqlalchemy import create_engine
import pandas as pd

# test


def data_exists(engine, table_name, dataframe, unique_columns=['place_name', 'date_id']) -> pd.DataFrame:
    """
    Check if data already exists in the database.
    :param engine: SQLAlchemy engine object.
    :param table_name: Name of the database table.
    :param unique_columns: List of column names to check for duplicates.
    :param dataframe: DataFrame with data to check.
    :return: DataFrame with new data that doesn't exist in the database.
    """
    with engine.connect() as connection:
        existing_data = pd.read_sql_table(
            table_name, con=engine, columns=unique_columns)
        if 'date_id' in unique_columns:
            dataframe['date_id'] = pd.to_datetime(
                dataframe['date_id']).dt.tz_convert('UTC')
            existing_data['date_id'] = pd.to_datetime(
                existing_data['date_id']).dt.tz_localize('UTC')
        merged = pd.merge(dataframe, existing_data,
                          on=unique_columns, how='left', indicator=True)
        new_data = merged[merged['_merge'] ==
                          'left_only'].drop('_merge', axis=1)
        return new_data


def save_to_postgres(dataframe, connection_url, table_name, unique_columns=['place_name', 'date_id']):
    """
    Save a Pandas DataFrame to a PostgreSQL table with a check for duplicates.
    :param dataframe: DataFrame to save.
    :param connection_url: Database URL (SQLAlchemy format).
    :param table_name: Name of the PostgreSQL table.
    :param unique_columns: List of column names to check for duplicates.
    """
    engine = create_engine(connection_url)

    # If table exists, check for duplicates based on unique columns
    new_data = data_exists(
        engine, table_name, dataframe, unique_columns)

    # Save only the new, non-duplicate data to the database
    if not new_data.empty:
        new_data.to_sql(table_name, con=engine,
                        if_exists='append', index=False)
        print(f"New data successfully saved to table '{table_name}'.")
    else:
        print(
            f"No new data to save. Table '{table_name}' is up-to-date.")
