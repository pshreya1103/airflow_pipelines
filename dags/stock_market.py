# from airflow import DAG
import logging
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
from airflow.hooks.base import BaseHook
import requests

from include.data.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME

SYMBOL = 'AAPL'
logger = logging.getLogger(__name__) 
@dag(
    start_date=datetime(2023, 9, 25),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=3000, mode='poke')
    def is_api_available(**kwargs) -> PokeReturnValue:
        api=BaseHook.get_connection('stock_api')
        logger.info(f"api: {api}")
        url=f"{api.host}{api.extra_dejson['endpoint']}{SYMBOL}{"?metrics=high?&interval=1d&range=1y"}"
        logger.info(f"url: {url}")
        headers = api.extra_dejson['headers']
        response=requests.get(url,headers=headers)
        condition = response.json()['chart']['error'] is None
        kwargs['ti'].xcom_push(key='url', value=url)
        return PokeReturnValue(is_done=condition,xcom_value=url)


    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        op_kwargs = {'url': '{{task_instance.xcom_pull(task_ids="is_api_available")}}','symbol': SYMBOL }
    )

    store_prices = PythonOperator(
        task_id = "store_prices",
        python_callable = _store_prices,
        op_kwargs = { 'stock': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}' }
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS' : '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path' : '{{task_instance.xcom_pull(task_ids="store_prices")}}'
        }
    )
    @task
    def load_to_dw(task_instance):
        # Retrieve the XCom value
        store_prices_value = task_instance.xcom_pull(task_ids='store_prices')

        # Use the retrieved value in the File path
        aql.load_file(
            task_id='load_to_dw',
            input_file=File(
                path=f"s3://{BUCKET_NAME}/{store_prices_value}",
                conn_id='minio'
            ),
            output_table=Table(
                name='stock_market',
                conn_id='postgres',
                metadata=Metadata(schema='public')
            )
        )
    
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw()

stock_market()