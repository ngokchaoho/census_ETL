from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, create_engine
import logging
import os

Base = declarative_base()

log = logging.getLogger(__name__)
environment_uri = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

# Create the database (if would only create if not exist)
log.info(environment_uri)
engine = create_engine(environment_uri,echo=True)
Base.metadata.create_all(engine)

class Census(Base):
    __tablename__ = "census"
    id = Column(Integer, primary_key=True, autoincrement=True)
    first = Column(String)
    last = Column(String)
    age = Column(Integer)
    state = Column(String)
    weight = Column(Float)
