import pandas as pd
from airflow.sdk import task, dag
import pendulum


@task(
    retries=2
)
def extract():
    from etl.extract import read_data #avoid top level code
    return read_data()


@task(
    retries=2
)
def validate(dataframe):
    from etl.validate import validate_contract
    return validate_contract(dataframe)

@task(
    retries=2
)
def transform(
    validated_dataframe
):
    from etl.transform import transform_data
    return transform_data(validated_dataframe)

@task(
    retries=2
)
def load(
    dataframe: pd.DataFrame,
):
    from etl.load import load_to_minio
    return load_to_minio(dataframe)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 29, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=3,
)
def dhap42():
    extracted_dataframe = extract()
    validated_dataframe = validate(extracted_dataframe)
    transformed_dataframe = transform(validated_dataframe)
    load(transformed_dataframe)

dhap42()