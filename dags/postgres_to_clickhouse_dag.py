from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os
from pathlib import Path
from dotenv import load_dotenv
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

DATA_DIR = "/opt/airflow/data/csv"
DB_CONN_STRING = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def load_csv_to_dag_database(ti):
    if not os.path.isdir(DATA_DIR):
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

    engine = create_engine(DB_CONN_STRING)
    csv_files = list(Path(DATA_DIR).glob("*.csv"))
    csv_file = csv_files[0]

    if not csv_file:
        print("No CSV files found in", DATA_DIR)
        return


    table_name = csv_file.stem
    table_final_name = f"{table_name}_table"

    df = pd.read_csv(csv_file)
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    with engine.connect() as conn:
        df.to_sql(
            name=table_final_name,
            con=conn,
            if_exists="replace",
            index=False
        )

    # ti.xcom_push(key='table_name', value = table_name)
    return table_name

@dag(
    dag_id="cleaned_postgres_to_clickhouse",
    start_date=datetime(2025, 8, 5),
    schedule=None,
    catchup=False,
    tags=["clickhouse", "postgres", "cleaned_ingest"],
)
def taskflow():

    load_csv_to_postgres = PythonOperator(
        task_id="load_csv_to_dag_database",
        python_callable=load_csv_to_dag_database,
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
    #     parameters={
    #     "table_name": "dag_database.{{ ti.xcom_pull(task_ids='load_csv_to_dag_database')}}"
    # }
    )

    with open("/opt/airflow/data/sql/create_clean_table.sql") as f:
        create_clean_table_sql = f.read()

    create_cleaned_table = PostgresOperator(
        task_id="create_cleaned_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=create_clean_table_sql
    )
    
    #creates an table in clickhouse with postgresEngine that is synced with the cleaned table in postgres
    with open("/opt/airflow/data/sql/create_ps_to_ch_conn.sql") as f:
        raw_sql = f.read()
        
    create_clickhouse_postgres_view_sql = raw_sql.format(
        TARGET_TABLE=TARGET_TABLE,
        TARGET_DB=TARGET_DB,
        CLICKHOUSE_POSTGRES_VIEW=CLICKHOUSE_POSTGRES_VIEW
    )

    create_clickhouse_postgres_view = ClickHouseOperator(
        
        task_id="create_clickhouse_postgres_view",
        sql=create_clickhouse_postgres_view_sql
    )


    #copies rows from local clickhouse table from prev task into the final clickhouse table
    with open("/opt/airflow/data/sql/ingest_data.sql") as f:
        raw_sql = f.read()


    ingest_data_sql = raw_sql.format(
        TARGET_TABLE=TARGET_TABLE,
        TARGET_DB=TARGET_DB,
        CLICKHOUSE_POSTGRES_VIEW=CLICKHOUSE_POSTGRES_VIEW
    )

    insert_cleaned_data = ClickHouseOperator(
        
        task_id="insert_cleaned_data_to_clickhouse",
        sql=ingest_data_sql
    )

    load_csv_to_postgres >> create_clickhouse_table >> create_cleaned_table >> create_clickhouse_postgres_view >> insert_cleaned_data

taskflow()