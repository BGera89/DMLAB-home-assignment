from fastapi.testclient import TestClient
# Replace with the name of your FastAPI app module
from api_fetcher_api import app

client = TestClient(app)


def test_fetch_and_save_weather():
    # Prepare the query parameters for testing
    params = {
        "lat": 52.52,  # Example latitude
        "lon": 13.405,  # Example longitude
        # "start_date": "2024-06-01",
        # "end_date": "2024-06-10",
        # "timezone": "Europe/Berlin",
        'place_name': 'TEST'  # Optional, default is "Europe/Berlin"
    }

    # Send a GET request to your /weather endpoint
    response = client.get("/weather", params=params)

    # Print the response to check what was returned
    print(response.json())  # You can check the full response here

    # Asserting the response status code and expected message
    # assert response.status_code == 200
    # assert response.json() == {
    #    "message": "Weather data successfully saved to the database."}


# Run the test
if __name__ == "__main__":
    test_fetch_and_save_weather()
