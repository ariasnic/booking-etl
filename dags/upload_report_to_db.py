import pandas as pd

from airflow.models import Variable
from model import Connection

def upload_report_csv_to_db(ti):
    file_path_to_upload = ti.xcom_pull(task_ids='transform_booking', key='output_filepath')
    report_to_upload = pd.read_csv(file_path_to_upload)
    connection = Connection(Variable.get("data_dev_connection"))
    engine = connection.get_engine()
    report_to_upload.to_sql('report', con=engine, index=False, if_exists='replace', method='multi')
    return 1