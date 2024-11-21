from data_access.data_write import save_to_postgres


def fetch_and_process_multiple(fetcher: object, processor_class: object,
                               fetch_method: str, process_method: str,
                               latitude: float, longitude: float, start_date: str,
                               end_date: str, timezone: str, place_name: str,
                               db_url: str, table_name: str):
    """
    Fetches data from a specified source, processes it, and saves the processed data to a PostgreSQL database.

    This function uses dynamic method invocation to fetch data and process it. It allows for flexible integration 
    of different fetchers and processors by specifying their respective methods.

    Parameters:
        fetcher (object): The object responsible for fetching data (e.g., an API client).
        processor_class (class): The class responsible for processing the fetched data.
        fetch_method (str): The name of the method in the fetcher to fetch data.
        process_method (str): The name of the method in the processor class to process the data.
        latitude (float): The latitude of the location for which data is to be fetched.
        longitude (float): The longitude of the location for which data is to be fetched.
        start_date (str): The start date for the data fetch in ISO format (e.g., "2024-01-01").
        end_date (str): The end date for the data fetch in ISO format (e.g., "2024-01-31").
        timezone (str): The timezone of the location (e.g., "UTC").
        place_name (str): A human-readable name for the location (e.g., "New York").
        db_url (str): The database connection URL for saving the processed data.
        table_name (str): The name of the table in the database where the data will be stored.

    Returns:
        None: The function saves the processed data to the database and does not return anything. 
              If an error occurs, it prints the error message and returns `None`.
    """

    try:
        # Fetch data using the specified method
        response = getattr(fetcher, fetch_method)(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone
        )

        # Process data using the specified method
        processor = processor_class(response=response, place_name=place_name)
        processed_data = getattr(processor, process_method)()
        save_to_postgres(processed_data, db_url, table_name)
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
