import os


DATASET_CSV_FILE_DIR = os.getenv("DATASET_CSV_FILE_DIR", "/opt/airflow/dags/data/datasets")
REPORT_CSV_FILE_DIR = os.getenv("REPORT_CSV_FILE_DIR", "/opt/airflow/dags/data/reports")