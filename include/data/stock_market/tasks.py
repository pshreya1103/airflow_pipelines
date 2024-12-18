import logging
import requests
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import json
from minio import Minio
from io import BytesIO

logger = logging.getLogger(__name__) 

BUCKET_NAME = 'stock-market'


def  _get_stock_prices(url, symbol):
     api = BaseHook.get_connection('stock_api')
     response = requests.get(url, headers=api.extra_dejson['headers'])
     return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
     client = _get_minio_client()
     if not client.bucket_exists(BUCKET_NAME):
          client.make_bucket(BUCKET_NAME)
     stock=json.loads(stock)
     symbol = stock['meta']['symbol']
     data =json.dumps(stock, ensure_ascii=False).encode('utf8')
     objw = client.put_object(
          bucket_name=BUCKET_NAME,
          object_name=f'{symbol}/prices.json',
          data=BytesIO(data),
          length=len(data)
     )
     result = f'{objw.bucket_name}/{symbol}'
     logger.info(result)
     return result

def _get_formatted_csv(path):
     client = _get_minio_client()
     prefix_name = f'{path.split('/')[1]}/formatted_prices'
     objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
     for obj in objects:
          if obj.object_name.endswith('.csv'):
               csv_path = f"s3://{BUCKET_NAME}/{obj.object_name}"
               logger.info(csv_path)
               return csv_path
     logger.log(prefix_name,objects)
     raise AirflowNotFoundException('The csv file does not exist')

def _get_minio_client():
     minio = BaseHook.get_connection('minio')
     client = Minio(
          endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
          access_key=minio.login,
          secret_key=minio.password,
          secure=False
     )
     return client