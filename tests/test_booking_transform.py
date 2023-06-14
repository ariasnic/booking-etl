from dags.booking_transform import DataProcessor
import pandas as pd

def test_convert_currency():
    # Create an instance of DataProcessor for testing
    data_processor = DataProcessor()

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
    result = data_processor.convert_currency(df_booking)

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

    assert result.equals(expected_result), "Currency conversion for pounds is incorrect."
    
