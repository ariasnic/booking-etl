from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from booking_transform import BookingTransform
from upload_report_to_db import UploadReportCSVToDB
from airflow.decorators import task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'generate_report',
    default_args=default_args,
    description='This DAG create a monthly statistics by restaurants report, output the csv and store the rows in a DB',
    schedule_interval="@monthly",
    start_date=days_ago(31),
    catchup=True
) as dag:
    create_report_table = PostgresOperator(
        task_id="create_report_table",
        postgres_conn_id="postgres_local",
        sql="""
            CREATE TABLE IF NOT EXISTS report (
            report_id SERIAL PRIMARY KEY,
            restaurant_id VARCHAR NOT NULL,
            restaurant_name VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            month VARCHAR NOT NULL,
            number_of_bookings INT NOT NULL,
            number_of_guests INT NOT NULL,
            amount FLOAT NOT NULL);
          """,
    )

    data_processor = BookingTransform()

    @task(task_id="transform_booking")
    def transform_booking():
        return data_processor.transform_booking_dataset()

    transform_booking = transform_booking()

    save_report_to_db = UploadReportCSVToDB()

    @task(task_id="upload_data_to_db")
    def upload_data_to_db():
        return save_report_to_db.upload_data()

    upload_data_to_db = upload_data_to_db()

    create_report_table >> transform_booking >> upload_data_to_db
