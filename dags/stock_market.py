# from airflow import DAG
import logging
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
from airflow.hooks.base import BaseHook
import requests

from include.data.stock_market.tasks import _get_stock_prices

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
        op_kwargs = {'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}','symbol': SYMBOL }
    )

    is_api_available() >> get_stock_prices

stock_market()