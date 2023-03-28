
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

default_args = {
  "owner": "airflow",
  "email_on_failure": False,
  "email_on_retry": False,
  "email": "admin@localhost.com",
  "retries": 1,
  "retry_delay": timedelta(minutes=3)
}

with DAG(
  # id must be unique across the airflow instance
  "forex_data_pipeline"
  , start_date = datetime(2023, 3, 26)
  , schedule_interval = "@daily"
  , default_args = default_args
  , catchup=False) as dag:

  is_forex_rates_available = HttpSensor(
    task_id = "is_forex_rates_available", # must be unique within the same dag
    http_conn_id = "forex_api",
    endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
    response_check = lambda response: "rates" in response.text,
    poke_interval = 5,
    timeout = 20 # after 20 seconds - if no response, it will fail
  )
