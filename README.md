# Run

'docker-compose build'
'docker-compose up'

## Set credentials :
__password__ doesn't have to contain the value __airflow__ in it, otherwise it will not be usable.

- Admin → Variables
    - Key : data_dev_connection
    - Value : postgresql+psycopg2://airflow:__password__@postgres/airflow
- Admin → Connections
    - Connection Id : postgres_local
    - Connection Type : Postgres
    - Host : postgres
    - Schema : airflow
    - Login : airflow
    - Password : __password__
    - Port : 5432

# Test
'pip install -r requirements_dev'
'export PYTHONPATH=/path/to/booking-etl'
'pytest # at the project root'

# Arbitrations and remarks
- I have chosen to implement only one dag, that includes the creation of the __report__ table. An other option would be to delegate the creation of the table to an other DAG, that will work as a migration of the DB.
- I implemented the DAG to run monthly and to create the report based on the most recent dataset, based on the filename format : __booking_yyyy_mm_dd.csv__. For the need of this assignement, the input dataset is renamed after this format, and stored in ./dags/datasets folder. But in an industrialization process, the __get_most_recent_file__ function could be adapted to retrieve the datasets from an AWS S3 Bucket for example.
- For this assigment, I assumed that the dataset contains 4 countries, 2 datetime formats, prices in € and pounds, unique combinations restaurant_id/restaurant_name/country. An improvment could be to test the dataset structure, and to raise an alert if anything new is added.
- The final dataset is stored as a table, but I can be an option to also store the CSV as a binary data in an other table of the DB.
- To test the data insertion, it is possible to add a task at the end of the DAG like this : 
    - test_report = PostgresOperator(task_id="test_report", postgres_conn_id="postgres_local", trigger_rule=TriggerRule.ALL_DONE, sql="SELECT * FROM report LIMIT 20;")
    - or to log in locally to the postgresql DB : 'psql -U airflow -h localhost -p 5432'


========================
Assignment: ETL pipeline
========================

Given the attached dataset (bookings.csv), we want to generate a w.

input dataset : bookings.csv

* booking_id
* restaurant_id
* restaurant_name
* client_id
* client_name
* amount
* Guests (number of people for the given booking)
* date
* country

Expected output dataset  : monthly_restaurants_report.csv

* restaurant_id
* restaurant_name
* country
* month (in following format : YYYY_MM)
* number_of_bookings
* number_of_guests
* amount

The goal of this assignment is to implement this transformation as a proper data engineering pipeline.

Constraints : 

* The final dataset must be dumped in a postgresql table
* The postgresql will be hosted in a docker container

Languages:

 * Python (with any library/framework you want)
 * SQL


It’s simple and relatively unguided on purpose, our criterias are the following : 

* We can make it work
* The output dataset is clean
* The pipeline is cut in well-structured steps, easy to re-run independently easy to maintain and evolve
* The code is clean and well-structured (naming, functions structuration, ...) : imagine you submit this code to your colleagues for review before release
* Optional : the code is production-ready (ie. all side aspects needed to industrialize a code : unit tests, exception management, logging, ...)
* Discussion in the README.md : you can write down explanations on how to make the pipeline run arbitrations you took 
* Limitations or things you didn’t have time to implement (we know doing a fully prod-ready script may take quite some time).
* Any extras you think are relevant
