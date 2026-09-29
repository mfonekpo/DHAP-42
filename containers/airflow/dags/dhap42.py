from airflow.sdk import task, dag
import pendulum
import pandas as pd


@task(
    retries=2
)
def extract():
    from etl.extract import read_data
    return read_data()


@task(
    retries=2
)
def validate(dataframe):
    from etl.validate import validate_contract
    return validate_contract(dataframe)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 29, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=3,
)
def dhap42():
    dataframe = extract()
    validate(dataframe)


dhap42()