FROM apache/airflow:3.3.0

# copy only the requirements.txt file to leverage Docker cache
COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt