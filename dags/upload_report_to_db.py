import csv
import pandas as pd

from airflow.models import Variable
from model import Report, Connection

class upload_report_csv_to_db():
    def upload_data():
        file_path_to_upload = ti.xcom_pull(task_ids='transform_booking', key='output_filepath')
        # report_to_upload = pd.read_csv(file_path_to_upload)
        # connection = Connection(Variable.get("data_dev_connection"))
        # engine = connection.get_engine()
        # report_to_upload.to_sql('report', con=engine, index=False, if_exists='append', method='multi')
        data_insert = []
        with open(file_path_to_upload, encoding='utf-8') as csvf:
            csv_reader = csv.DictReader(csvf)
            for row in csv_reader:
                report_data = Report(
                                    restaurant_id=row['restaurant_id'],
                                    restaurant_name=row['restaurant_name'],
                                    country=row['country'],
                                    month=row['month'],
                                    number_of_bookings=row['number_of_bookings'],
                                    number_of_guests=row['number_of_guests'],
                                    amount=row['amount'])
                data_insert.append(report_data)

        connection = Connection(Variable.get("data_dev_connection"))
        session = connection.get_session()
        session.bulk_save_objects(data_insert)
        session.commit()
        session.close()
        return 1