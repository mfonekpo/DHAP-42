FROM apache/airflow:3.3.2

# copy only the requirements.txt file to leverage Docker cache
COPY requirements.txt /requirements.txt

RUN python3 -m pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt