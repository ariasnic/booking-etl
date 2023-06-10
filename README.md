Assignment: ETL pipeline
========================

Given the attached dataset (bookings.csv), we want to generate a report with monthly statistics by restaurants.

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
