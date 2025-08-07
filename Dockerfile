FROM apache/airflow:3.0.3
USER airflow
RUN pip install "apache-airflow-providers-postgres==5.13.1"
RUN pip install --no-cache-dir pandas==2.1.4 psycopg2-binary
RUN pip install -U airflow-clickhouse-plugin

