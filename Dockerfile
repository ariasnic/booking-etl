FROM apache/airflow:2.6.1
WORKDIR /booking-etl
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
COPY . .