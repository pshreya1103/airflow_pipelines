from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
	'start_date' : datetime(2024,1,1),
    'retries': 1
}

dag = DAG(
    'second_dag',
    default_args=default_args,
    description='A simple DAG for extracting and loading data',
    schedule_interval='@daily',  # Runs daily
    catchup=False
)

def sub():
	print('sub opeartion')
	return 1

def add():
	print('add opeartion')
	return 0

add = PythonOperator(
		dag=dag,
        task_id='add',
        python_callable=add
    )

sub = PythonOperator(
        dag=dag,
		task_id='sub',
        python_callable=sub
    )

sub >> add