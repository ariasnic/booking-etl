import csv
import logging
from airflow.models import Variable
from model import Report, Connection
from airflow.operators.python import get_current_context

class UploadReportCSVToDB:
    def upload_data(self):
        context = get_current_context()
        ti = context["ti"]
        file_path_to_upload = ti.xcom_pull(task_ids='transform_booking', key='output_filepath')

        data_insert = []
        with open(file_path_to_upload, encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                report_data = Report(
                    restaurant_id=row['restaurant_id'],
                    restaurant_name=row['restaurant_name'],
                    country=row['country'],
                    month=row['month'],
                    number_of_bookings=row['number_of_bookings'],
                    number_of_guests=row['number_of_guests'],
                    amount=row['amount']
                )
                data_insert.append(report_data)

        session = None

        try:
            connection = Connection(Variable.get("data_dev_connection"))
            session = connection.get_session()
            session.bulk_save_objects(data_insert)
            session.commit()
        except Exception as e:
            logging.info("Error on csv export : %s", e)
        finally:
            if session is not None:
                session.close()
        return 