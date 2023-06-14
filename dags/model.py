import uuid
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Date


class Connection(object):

    def __init__(self, db_connection):
        engine = create_engine(db_connection)
        self.engine = engine

    def get_session(self):
        Session = sessionmaker(bind=self.engine)

        return Session()

    def get_engine(self):
        return self.engine


Base = declarative_base()


class Report(Base):
    __tablename__ = 'report'
    
    report_id = Column(Integer, primary_key=True)
    restaurant_id = Column(String)
    restaurant_name = Column(String)
    country = Column(String)
    month = Column(DateTime)
    number_of_bookings = Column(Integer)
    number_of_guests = Column(Integer)
    amount = Column(Float)

    def __init__(self, report_id, restaurant_id, restaurant_name, country, month, number_of_bookings, number_of_guests, amount):
        self.report_id = report_id
        self.restaurant_id = restaurant_id
        self.restaurant_name = restaurant_name
        self.country = country
        self.month = month
        self.number_of_bookings = number_of_bookings
        self.number_of_guests = number_of_guests
        self.amount = amount