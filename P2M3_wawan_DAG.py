import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import psycopg2 as db
import logging
from elasticsearch import Elasticsearch
import os



# Step 1: Fetch data from PostgreSQL
def fetch_from_postgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_wawan_data_raw.csv', sep=',', index=False)

    conn.close()


# Step 2: Clean data
def clean_data():
    logging.info("Cleaning data")
    df = pd.read_csv('/opt/airflow/dags/P2M3_wawan_data_raw.csv')
    
    # Drop duplicates
    df.drop_duplicates(inplace=True)

    # Normalize column names
    df.columns = [col.lower().replace(' ', '_').strip('|').replace(' ', '_') for col in df.columns]

    # Handle missing values
    df.fillna(value={'column1': 'default_value', 'column2': 0}, inplace=True)
    
    # Save cleaned data
    df.to_csv('/opt/airflow/dags/P2M3_wawan_data_clean.csv', index=False)
    logging.info("Data cleaned and saved to CSV")



# Step 3: Post to Elasticsearch
def post_to_elasticsearch():
    logging.info("Posting data to Elasticsearch")
    es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
    df = pd.read_csv('/opt/airflow/dags/P2M3_wawan_data_clean.csv')
    
    for i, row in df.iterrows():
        doc=row.to_json()
        es.index(index='table_m3_postgresql', doc_type='doc', id=i, body=doc)
    logging.info("Data posted to Elasticsearch")



# Default arguments
default_args = {
    'owner': 'wawan',
    'start_date': dt.datetime(2024, 7, 21, 10, 40, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'P2M3_wawan_DAG',
    default_args=default_args,
    description='Fetch from PostgreSQL, clean data, and post to Elasticsearch',
    schedule_interval='30 6 * * *',  # Set to run every day at 06:30
) as dag:
    
    fetch_data_task = PythonOperator(task_id='fetch_from_postgresql',
                                     python_callable=fetch_from_postgresql)
    
    clean_data_task = PythonOperator(task_id='clean_data', 
                                     python_callable=clean_data)
    
    
    post_to_es_task = PythonOperator(task_id='post_to_elasticsearch',
                                     python_callable=post_to_elasticsearch)

# Define task dependencies
fetch_data_task >> clean_data_task >> post_to_es_task
