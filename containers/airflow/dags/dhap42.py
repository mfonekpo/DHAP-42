from airflow.sdk import task, dag
import pendulum
import pandas as pd


@task(
    retries=3
)
def extract():
    from etl.extract import read_data
    return read_data()


@task(
    retries=3
)
def check_read_data(dataframe: pd.DataFrame):
    print(dataframe)
    # return read_data


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 29, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=3,
)
def dhap42():
    dataframe = extract()
    check_read_data(dataframe)


dhap42()