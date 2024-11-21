import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from api_fetcher_api import WeatherDataFetcher, WeatherDataProcessor  # Adjust as needed


class TestWeatherDataFetcher(unittest.TestCase):
    """
    Unit tests for the WeatherDataFetcher class.
    """

    # Mock the API client
    @patch('weather_data_fetcher.openmeteo_requests.Client')
    def test_fetch_daily_weather_data(self, MockClient):
        """
        Test the fetch_daily_weather_data method of WeatherDataFetcher.
        """
        # Arrange
        mock_client = MockClient.return_value
        mock_client.weather_api.return_value = [{"Daily": MagicMock()}]

        fetcher = WeatherDataFetcher()
        fetcher.client = mock_client

        # Act
        result = fetcher.fetch_daily_weather_data(
            latitude=52.52, longitude=13.405,
            start_date="2024-06-01", end_date="2024-06-10"
        )

        # Assert
        mock_client.weather_api.assert_called_once_with(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": 52.52,
                "longitude": 13.405,
                "start_date": "2024-06-01",
                "end_date": "2024-06-10",
                "daily": ["temperature_2m_mean", "rain_sum",
                          "wind_speed_10m_max", "shortwave_radiation_sum"],
                "timezone": "Europe/Berlin",
            }
        )
        self.assertEqual(result, mock_client.weather_api.return_value[0])


class TestWeatherDataProcessor(unittest.TestCase):
    """
    Unit tests for the WeatherDataProcessor class.
    """

    def test_process_daily_data(self):
        """
        Test the process_daily_data method of WeatherDataProcessor.
        """
        # Arrange
        mock_response = MagicMock()
        mock_daily = MagicMock()
        mock_daily.Variables.return_value.ValuesAsNumpy.side_effect = [
            [15.0, 16.0, 17.0],  # temperature_2m_mean
            [0.0, 0.1, 0.2],     # rain_sum
            [3.0, 4.0, 5.0],     # wind_speed_10m_max
            [100.0, 200.0, 300.0]  # shortwave_radiation_sum
        ]
        mock_response.Daily.return_value = mock_daily
        mock_daily.Time.return_value = [
            1690000000, 1690003600, 1690007200]  # Mock timestamps
        mock_daily.TimeEnd.return_value = [1690007200]  # Mock end timestamps
        mock_daily.Interval.return_value = 3600

        processor = WeatherDataProcessor(
            response=mock_response, place_name="Berlin")

        # Act
        result = processor.process_daily_data()

        # Assert
        expected_data = {
            "place_name": ["Berlin", "Berlin", "Berlin"],
            "date_id": pd.to_datetime([1690000000, 1690003600, 1690007200], unit="s", utc=True),
            "temperature_2m_mean": [15.0, 16.0, 17.0],
            "rain_sum": [0.0, 0.1, 0.2],
            "wind_speed_10m_max": [3.0, 4.0, 5.0],
            "shortwave_radiation_sum": [100.0, 200.0, 300.0]
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result, expected_df)


if __name__ == "__main__":
    unittest.main()
