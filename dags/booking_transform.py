from logging import Logger
import requests
import argparse
import os

from datetime import timedelta, datetime
from currency_converter import CurrencyConverter
import pandas as pd

import config

rename_header = { 
                  'date': 'month',
                  'booking_id': 'number_of_bookings',
                  'guests': 'number_of_guests', 
                  'amount_value': 'amount'
                }
reposition_header = ['timestamp', 'date_time', 'traffic_index', 'jams_count', 'jams_length', 'jams_delay', 'traffic_index_weekago']

currency = {'£': 'GBP', '€': 'EUR'}
country_translation = {'España': 'Spain', 'Italia': 'Italy'}


def get_symbol(price):
        import re
        pattern =  r'(\D*)[\d\,\.]+(\D*)'
        g = re.match(pattern, price.strip()).groups()
        return (g[0] or g[1]).strip()

def get_amount(price):
        import re
        pattern =  r'\D*([\d\,\.]+)\D*'
        g = re.match(pattern, price.strip()).groups()
        return g[0].strip()


def convert_currency(df_booking: pd.DataFrame) -> pd.DataFrame:
    # extract currency from amount
    df_booking['amount_symbol'] = df_booking['amount'].apply(lambda x : get_symbol(x))
    df_booking['currency'] = df_booking['amount_symbol'].map(currency)
    # extract numerical value from amount
    df_booking['amount_value'] = df_booking['amount'].apply(lambda x : get_amount(x))
    print(df_booking['amount_value'].head())
    df_booking['amount_value'] = df_booking['amount_value'].str.replace(',', '.')
    # convert 
    c = CurrencyConverter(decimal=False)
    df_booking.loc[df_booking['currency'] != 'EUR', 'amount_value'] = df_booking.loc[df_booking['currency'] != 'EUR'].apply(lambda x : c.convert(x.amount_value, x.currency, 'EUR'), axis=1)
    return df_booking

def get_most_recent_file():
    dataset_list = []
    for filename in os.listdir(config.DATASET_CSV_FILE_DIR):
        if filename.endswith('.csv'):
            dataset_list.append(filename)
    # sort to retrieve most recent dataset path
    most_recent_file_name = sorted(dataset_list, reverse=True).pop(0)
    return most_recent_file_name


def save_report_to_csv(df_booking, most_recent_filename, ti):
    output_filename = 'report_' + most_recent_filename
    output_filepath = os.path.join(config.REPORT_CSV_FILE_DIR, output_filename)
    try:
        df_booking.to_csv(output_filepath, encoding='utf-8', index=False)
        # xcom push filename to use it in next task
        ti.xcom_push(key='output_filepath', value=output_filepath)

    except Exception:
        Logger.info("Error on csv export")


def transform_booking_dataset(ti):
    most_recent_file_name = get_most_recent_file()
    most_recent_file_path = os.path.join(config.DATASET_CSV_FILE_DIR, most_recent_file_name)
    df_booking = pd.read_csv(most_recent_file_path)
    df_booking = convert_currency(df_booking)
    df_booking['country'] = df_booking['country'].map(country_translation)
    # convert date to datetime format
    df_booking['date'] = pd.to_datetime(df_booking['date'])
    # agg
    print(df_booking.head(20))
    df_report = df_booking.groupby(['restaurant_id', 'restaurant_name', 'country', pd.Grouper(key='date', freq="M")]).agg(
        {
            'booking_id': 'count',
            'guests': 'sum',
            'amount_value': 'sum',
        }
    ).reset_index()
    pd.set_option('display.max_columns', None)
    print(df_report.head(20))
    # convert datetime to YYYY-MM
    df_report['date'] = df_report['date'].dt.strftime('%Y-%m')
    print(df_report.head(20))
    df_report = df_report.rename(columns = rename_header)
    save_report_to_csv(df_report, most_recent_file_name, ti)
    return df_report
    