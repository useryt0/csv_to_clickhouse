from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from minio import Minio
from datetime import datetime
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO
from airflow.operators.python import get_current_context



import os
import pandas as pd
load_dotenv()


POSTGRES_HOST = os.getenv('POSTGRES_HOST')  
POSTGRES_PORT = os.getenv('POSTGRES_PORT')  
POSTGRES_DB = os.getenv('POSTGRES_DB') 
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
SOURCE_VIEW = os.getenv('SOURCE_VIEW') #cleaned table in postgres
TARGET_TABLE = os.getenv('TARGET_TABLE') #destination table in clickhouse
TARGET_DB = os.getenv('TARGET_DB') #destination db in clickhouse
POSTGRES_CONN_ID = os.getenv('POSTGRES_CONN_ID') #connection to postgres in airflow
CLICKHOUSE_POSTGRES_VIEW = os.getenv('CLICKHOUSE_POSTGRES_VIEW')
# MINIO_URL=f"http://minio:9000/csv-file/{file_name}"
DATA_DIR = "/opt/airflow/data/csv"
DB_CONN_STRING = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

BUCKET_NAME = 'csv-file'
BUCKET_FILE_NAME = 'bucket_file_name'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _store_csv():
    client = _get_minio_client()
    
    csv_files = list(Path(DATA_DIR).glob("*.csv"))
    
    if not csv_files:
        print("No CSV files found in", DATA_DIR)
        return


    csv_file = csv_files[0]
    table_name = csv_file.stem
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        
    with open(csv_file, "rb") as f:
        file_data = f.read()
        file_size = len(file_data)

    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{table_name}",
        data=BytesIO(file_data),
        length=file_size,
        content_type="text/csv"
    )
    print(f"Uploaded {csv_file} to bucket {BUCKET_NAME} as {table_name}.csv")
    context = get_current_context()
    context['ti'].xcom_push(key='file_name', value=f"{table_name}")



@dag(
    dag_id="csv_to_clickhouse",
    start_date=datetime(2025, 8, 5),
    schedule=None,
    catchup=False,
    tags=["clickhouse", "csv","minio", "cleaned_ingest"],
)
def taskflow():


    load_csv_to_minio = PythonOperator(
        task_id="load_csv_to_minio",
        python_callable=_store_csv
    ) 
    
    with open("/opt/airflow/data/sql/create_table.sql") as f:
        create_table_raw_sql = f.read()
        create_table_sql = create_table_raw_sql.format(
        TARGET_TABLE=TARGET_TABLE,
        TARGET_DB=TARGET_DB
    )
        
    create_clickhouse_table = ClickHouseOperator(
        task_id="create_clickhouse_table",
        sql=create_table_sql
    )

    @task
    def render_sql(ti):
        file_name = ti.xcom_pull(task_ids="load_csv_to_minio", key="file_name")
        minio_url = f"http://minio:9000/csv-file/{file_name}"
        with open("/opt/airflow/data/sql/ingest_data_from_bucket.sql") as f:
            sql = f.read()
        return sql.replace("{{ params.minio_url }}", minio_url).replace("{{ params.TARGET_DB }}", TARGET_DB).replace("{{ params.TARGET_TABLE }}", TARGET_TABLE)
    rendered_sql_task= render_sql()

    
    insert_cleaned_bucket_data = ClickHouseOperator(
        
        task_id="insert_cleaned_bucket_data",
        sql=rendered_sql_task,
    ) 
    load_csv_to_minio >> create_clickhouse_table >> rendered_sql_task >> insert_cleaned_bucket_data

taskflow()