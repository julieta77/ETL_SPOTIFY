FROM apache/airflow:2.6.1
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt