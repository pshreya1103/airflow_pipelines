from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from airflow import DAG

def read_csv_file():
    df=pd.read_csv('include/data/dataset/melb_data.csv')
    print(df)
    return df.to_json()

def remove_null_values(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    df = pd.read_json(json_data)
    df = df.dropna()
    print(df)
    return df.to_json()

with DAG(
    dag_id = 'ingest_melbourne_housing_data',
    description = 'Ingesting melbourne housing data to transform',
    start_date=datetime(2023, 9, 25),
    schedule_interval='@daily',
    tags = ['python', 'transform', 'pipeline'],
    catchup=False
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file,
        retries = 1
    )

    remove_null_values = PythonOperator(
    task_id = 'remove_null_values',
    python_callable=remove_null_values,
    retries = 1
    )

    end = DummyOperator(
        task_id='end'
    )

start >> read_csv_file >> remove_null_values >> end