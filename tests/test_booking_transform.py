from unittest import TestCase, main
from unittest.mock import patch, MagicMock, Mock
from functools import wraps
import sys
sys.path.append('..')
from dags.booking_transform import BookingTransform


import pandas as pd

class TestBookingTransform(TestCase):
    def setUp(self):




        # Create an instance of BookingTransform for testing
        self.data_processor = BookingTransform()

    def test_convert_currency(self):
        # Create a mock TaskInstance object
        mock_logger = MagicMock()

        # Set the return value of the mocked get_current_context() function
        mock_logger.return_value = None
        # Create a sample DataFrame for testing
        df_booking = pd.DataFrame(
            {
                'booking_id': ['booking_id_1', 'booking_id_2', 'booking_id_3'],
                'restaurant_id': ['restaurant_id_1', 'restaurant_id_2', 'restaurant_id_3'],
                'restaurant_name': ['restaurant_name_1', 'restaurant_name_2', 'restaurant_name_3'],
                'client_id': ['client_id_1', 'client_id_2', 'client_id_3'],
                'client_name': ['client_name_1', 'client_name_2', 'client_name_3'],
                'amount': ['£10.50', '£20.75', '£30.90'],
                'guests': [9, 4, 8],
                'date': ['11/01/2017', '10-08-2016', '29/01/2015'],
                'country': ['country_1', 'country_2', 'country_3']
            }
        )

        # Perform the conversion
        result = self.data_processor.convert_currency(df_booking)

        # Assert the expected result
        expected_result = pd.DataFrame(
            {
                'amount': ['£10.50', '£20.75', '£30.90'],
                'amount_symbol': ['£', '£', '£'],
                'currency': ['GBP', 'GBP', 'GBP'],
                'amount_value': [11.88, 23.48, 34.97],
                'booking_id': ['booking_id_1', 'booking_id_2', 'booking_id_3'],
                'restaurant_id': ['restaurant_id_1', 'restaurant_id_2', 'restaurant_id_3'],
                'restaurant_name': ['restaurant_name_1', 'restaurant_name_2', 'restaurant_name_3'],
                'client_id': ['client_id_1', 'client_id_2', 'client_id_3'],
                'client_name': ['client_name_1', 'client_name_2', 'client_name_3'],
                'guests': [9, 4, 8],
                'date': ['11/01/2017', '10-08-2016', '29/01/2015'],
                'country': ['country_1', 'country_2', 'country_3']
            }
        )

        # Sort columns in both DataFrames
        result = result.reindex(sorted(result.columns), axis=1)
        expected_result = expected_result.reindex(sorted(expected_result.columns), axis=1)

        self.assertTrue(result.equals(expected_result), "Currency conversion for pounds is incorrect.")
    
    def test_get_most_recent_file(self):
        # Create a mock TaskInstance object
        mock_logger = MagicMock()

        # Set the return value of the mocked get_current_context() function
        mock_logger.return_value = None
        # Call the function to get the most recent file. Sample files are present in /tests/data/datasets
        result = self.data_processor.get_most_recent_file()

        # Get the expected result (most recent file)
        expected_result = 'booking_2023_06_12.csv'

        # Assert the expected result
        self.assertEqual(result, expected_result, "Most recent file not found.")
    
    @patch("dags.booking_transform.get_current_context")
    def test_transform_booking_dataset(self, mock_get_current_context):
        # Create a mock TaskInstance object
        mock_ti = MagicMock()

        # Set the return value of the mocked get_current_context() function
        mock_get_current_context.return_value = {"ti": mock_ti}

        # Create a mock TaskInstance object
        mock_logger = MagicMock()

        # Set the return value of the mocked get_current_context() function
        mock_logger.return_value = None
        # Perform the transformation
        result = self.data_processor.transform_booking_dataset()


        # Assert the expected result
        expected_result = pd.DataFrame(
            {
                'restaurant_id': ['restaurant_id_1', 'restaurant_id_1', 'restaurant_id_2', 'restaurant_id_2', 'restaurant_id_3', 'restaurant_id_3'],
                'restaurant_name': ['restaurant_name_1', 'restaurant_name_1', 'restaurant_name_2', 'restaurant_name_2', 'restaurant_name_3', 'restaurant_name_3'],
                'country': ['United Kingdom', 'United Kingdom', 'France', 'France', 'Italy', 'Italy'],
                'month': ['2017_03', '2019_06', '2016_08', '2018_09', '2017_01', '2017_09'],
                'number_of_bookings': [3, 5, 2, 2, 2, 2],
                'number_of_guests': [19, 26, 10, 8, 15, 14],
                'amount': [81.18, 111.07, 42, 44, 96.65, 90.25],
            }
        )

        # Sort columns in both DataFrames
        result = result.reindex(sorted(result.columns), axis=1)
        expected_result = expected_result.reindex(sorted(expected_result.columns), axis=1)

        # Sort rows based on 'month' column
        result = result.sort_values('month').reset_index(drop=True)
        expected_result = expected_result.sort_values('month').reset_index(drop=True)

        pd.set_option('display.max_columns', None)

        print("Result DataFrame:")
        print(result)
        print("Expected Result DataFrame:")
        print(expected_result)
        #mock_get_current_context.assert_called_once()
        assert result.equals(expected_result), "Booking dataset transformation is incorrect."


if __name__ == '__main__':
    main()