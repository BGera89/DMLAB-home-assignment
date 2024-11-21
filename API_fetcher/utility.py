from data_access.data_write import save_to_postgres


def fetch_and_process_multiple(fetcher, processor_class,
                               fetch_method, process_method,
                               latitude, longitude, start_date,
                               end_date, timezone, place_name,
                               db_url, table_name):
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
