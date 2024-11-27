# from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
from airflow.hooks.base import BaseHook
import requests

from include.data.stock_market.tasks import _get_stock_prices

SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2023, 9, 25),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=3000, mode='poke')
    def is_api_availabe() -> PokeReturnValue:
        api=BaseHook.get_connection('stock_api')
        url=f"{api.host}{api.extra_dejson['endpoint']}"
        # url = 'https://query1.finance.yahoo.com/'
        response=requests.get(url,headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition,xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id = 'get_stock_prices',
        python_callable = _get_stock_prices,
        op_kwargs = {'url': 'https://query1.finance.yahoo.com/' ,'symbol': SYMBOL }
        # op_kwargs = {'url': '{{ task_instance.xcom_pull(task_ids="is_api_available")}}' ,'symbol': SYMBOL }
    )

    is_api_availabe() >> get_stock_prices

stock_market()