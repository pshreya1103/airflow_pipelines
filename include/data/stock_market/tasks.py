import logging
import requests
from airflow.hooks.base import BaseHook
import json

logger = logging.getLogger(__name__) 

def  _get_stock_prices(url, symbol):
     api = BaseHook.get_connection('stock_api')
     response = requests.get(url, headers=api.extra_dejson['headers'])
     return json.dumps(response.json()['chart']['result'][0])

