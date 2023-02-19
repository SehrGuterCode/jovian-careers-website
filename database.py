import os
from sqlalchemy import create_engine, text
from flask import jsonify

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string,
                       connect_args={"ssl": {
                         "ssl_ca": "/etc/ssl/cert.pem"
                       }})


def load_jobs_result():
  with engine.connect() as conn:
    return conn.execute(text("SELECT * FROM jobs")).all()


def load_jobs_from_db():
  jobs = [column._mapping for column in load_jobs_result()]
  return jobs


def load_jobs_as_json():
  results = load_jobs_result()
  results = [tuple(row) for row in results]
  return jsonify(results)
