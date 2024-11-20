from sqlalchemy import create_engine
import pandas as pd


def save_to_postgres(dataframe, db_url, table_name):
    """
    Save a Pandas DataFrame to a PostgreSQL table.
    :param dataframe: DataFrame to save.
    :param db_url: Database URL (SQLAlchemy format).
    :param table_name: Name of the PostgreSQL table.
    """
    try:
        # Create SQLAlchemy engine
        engine = create_engine(db_url)

        # Write the DataFrame to the database
        dataframe.to_sql(table_name, con=engine,
                         if_exists='append', index=False)

        print(f"Data successfully saved to table '{table_name}'.")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")
