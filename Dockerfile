FROM apache/airflow:2.10.3
RUN pip install apache-airflow
RUN pip install apache-airflow-providers-postgres

