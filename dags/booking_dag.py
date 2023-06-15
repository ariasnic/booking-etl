from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from booking_transform import BookingTransform
from upload_report_to_db import upload_report_csv_to_db
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
    description='',
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
    def transform_booking(task_instance):
        return data_processor.transform_booking_dataset(ti=task_instance)

    transform_booking = transform_booking()

    upload_data_to_db = PythonOperator(task_id='upload_data_to_db', python_callable=upload_report_csv_to_db)

    test_report = PostgresOperator(task_id="test_report", postgres_conn_id="postgres_local", trigger_rule=TriggerRule.ALL_DONE, sql="SELECT * FROM report LIMIT 20;")

    create_report_table >> transform_booking >> upload_data_to_db >> test_report

    if __name__ == "__main__":
        dag.test()