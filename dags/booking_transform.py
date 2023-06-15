import logging
import os

from currency_converter import CurrencyConverter
import pandas as pd
import re
from airflow.operators.python import get_current_context

import config


class BookingTransform:
    def __init__(self):
        self.rename_header = { 
            'date': 'month',
            'booking_id': 'number_of_bookings',
            'guests': 'number_of_guests', 
            'amount_value': 'amount'
        }

        self.currency = {'£': 'GBP', '€': 'EUR'}
        self.country_translation = {'España': 'Spain', 'Italia': 'Italy'}

    def get_symbol(self, price):
        pattern =  r'(\D*)[\d\,\.]+(\D*)'
        g = re.match(pattern, price.strip()).groups()
        return (g[0] or g[1]).strip()

    def get_amount(self, price):
        pattern =  r'\D*([\d\,\.]+)\D*'
        g = re.match(pattern, price.strip()).groups()
        return g[0].strip()

    def convert_currency(self, df_booking: pd.DataFrame) -> pd.DataFrame:
        # extract currency from amount
        df_booking['amount_symbol'] = df_booking['amount'].apply(lambda x: self.get_symbol(x))
        df_booking['currency'] = df_booking['amount_symbol'].map(self.currency)
        # extract numerical value from amount
        df_booking['amount_value'] = df_booking['amount'].apply(lambda x: self.get_amount(x))
        df_booking['amount_value'] = df_booking['amount_value'].str.replace(',', '.')
        # convert 
        c = CurrencyConverter()
        df_booking.loc[df_booking['currency'] != 'EUR', 'amount_value'] = df_booking.loc[df_booking['currency'] != 'EUR'].apply(lambda x: c.convert(x.amount_value, x.currency, 'EUR'), axis=1)
        df_booking['amount_value'] = pd.to_numeric(df_booking['amount_value']).round(2)
        return df_booking

    def get_most_recent_file(self):
        dataset_list = []
        for filename in os.listdir(config.DATASET_CSV_FILE_DIR):
            if filename.endswith('.csv'):
                dataset_list.append(filename)
        # sort to retrieve most recent dataset path
        most_recent_file_name = sorted(dataset_list, reverse=True).pop(0)
        return most_recent_file_name

    def save_report_to_csv(self, df_report, most_recent_filename):
        output_filename = 'report_' + most_recent_filename
        output_filepath = os.path.join(config.REPORT_CSV_FILE_DIR, output_filename)
        context = get_current_context()
        ti = context["ti"]
        try:
            df_report.to_csv(output_filepath, encoding='utf-8', index=False)
            # xcom push filename to use it in next task
            ti.xcom_push(key='output_filepath', value=output_filepath)
        except Exception as e:
            logging.info("Error on csv export : %s", e)

    def transform_booking_dataset(self):
        most_recent_file_name = self.get_most_recent_file()
        most_recent_file_path = os.path.join(config.DATASET_CSV_FILE_DIR, most_recent_file_name)
        df_booking = pd.read_csv(most_recent_file_path)
        df_booking = self.convert_currency(df_booking)
        df_booking['country'] = df_booking['country'].replace(self.country_translation)
        # convert date to datetime format, possibility to do it with regex
        df_booking['date'] = pd.to_datetime(df_booking['date'], format='%d/%m/%Y', errors='coerce').fillna(pd.to_datetime(df_booking['date'], format='%d-%m-%Y', errors="coerce"))
        pd.set_option('display.max_columns', None)

        print("df_booking DataFrame:")
        print(df_booking)
        # agg to create the report CHANGE
        df_report = df_booking.groupby([pd.Grouper(key='date',freq="M"),'restaurant_id', 'restaurant_name', 'country']).agg(
        {
            'booking_id': 'count',
            'guests': 'sum',
            'amount_value': 'sum',
        }
        ).reset_index()
        df_report['amount_value'] = pd.to_numeric(df_report['amount_value']).round(2)
        # convert datetime to YYYY_MM
        df_report['date'] = df_report['date'].dt.strftime('%Y_%m')
        df_report = df_report.rename(columns=self.rename_header)
        self.save_report_to_csv(df_report, most_recent_file_name)
        return df_report