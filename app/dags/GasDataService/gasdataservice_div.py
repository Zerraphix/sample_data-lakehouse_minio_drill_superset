from typing import List
import os    
import pandas as pd
from minio import Minio
from io import BytesIO
import json                               # contact to Operative System

import airflow
from airflow.decorators import dag, task

from decouple import config                 # secret configs 
import requests                             #  for making HTTP requests
import time
from datetime import datetime               # for date and time manipulation
from datetime import timedelta        # time, time time

from operator import itemgetter             # for sorting dicts
#from geopy.distance import distance         # calculate distance between geopos

import csv                                  # writing csv files

import json
import pendulum

## inspired by https://stackoverflow.com/questions/48393065/run-apache-airflow-dag-without-apache-airflow
if __name__ != "__main__":
    from airflow.decorators import dag, task
else:
    mock_decorator = lambda f=None,**d: f if f else lambda x:x
    dag = mock_decorator
    task = mock_decorator


def pull_data(service: str, data_dir: str, data_name: str, data_timedate: datetime, page_size: int, params: dict) -> List[str]:
    page_index = 0

    fNames = []
    
    while True:
        if not 'limit' in params.keys():
            params['limit'] = page_size
        params['offset'] = page_index * page_size
        
        r = requests.get(URL+service, params=params)
        if r.status_code != requests.codes.ok: # response 200 etc
            print(r)
            #print(r.headers)
            print(r.text)
            print(params)
            raise Exception('http not 200 ok', r.text)
            break
        else:
            rjson = r.json() # Extract JSON object
            print(f"Total number of records: {len(rjson['records'])}")
            # nFeatures += len(rjson['features'])
            #print(rjson)
            print(params)
        
        if len(rjson['records']) > 0:
            page_index += 1
            time_stamp = data_timedate.isoformat(timespec='seconds').replace(':', '.')
            fName = f'{data_dir}/{data_name}_{time_stamp}_#{page_index}.json'
            with open(fName, "w+") as f:
                f.write(r.text)
            fNames.append(fName)
        else:
            break
        time.sleep(1)
    return fNames




def setups():
    global URL, data_dir, page_size
    URL = 'https://api.energidataservice.dk/'
    data_dir = './dags/GasDataService/data'
    page_size = 1000
    

default_task_args = {
    'retries' : 10,
    'retry_delay' : timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}


@task
def extract_GasProdex(**kwargs):
    global URL, data_dir, page_size
    service = 'dataset/EntryExitGasquality'
    
    params = {}
    page_size = 2

    #print('kwargs:', kwargs)
    # for k, v in kwargs.items():
    #     print(k, '=', v)

    ts = datetime.fromisoformat(kwargs['ts'])

    params['start'] = kwargs['data_interval_start'].replace(tzinfo=None).isoformat(timespec='minutes')
    params['end']   = kwargs['data_interval_end'].replace(tzinfo=None).isoformat(timespec='minutes')

    print(params['start'], params['end'])

    return pull_data(service, data_dir, 'GasProdex', ts, page_size, params)

@task
def write_to_bucket(eProdex_jsons, table_path):
    MINIO_BUCKET_NAME = 'gasprodex-data'

    MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
    MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    # print(client.list_buckets())

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    for prodex_json_filepath in eProdex_jsons:
        
        # df = pd.DataFrame(tweet_list)
        # file_data = df.to_parquet(index=False)
        with open(prodex_json_filepath, 'r') as jf:
            prodex_json = json.load(jf)
        # rec_list = prodex_json['records']
        # df = pd.read_json(prodex_json_filepath)
        df = pd.DataFrame(prodex_json['records'])
        print(df)
        file_data = df.to_parquet(index=False)

        prodex_filename = prodex_json_filepath.split('/')[-1]
        # Put parquet data in the bucket
        filename = (
            # f"tweets/{batchDatetime.strftime('%Y/%m/%d')}/elon_tweets_{batchDatetime.strftime('%H%M%S')}_{batchId}.parquet"
            f"{table_path}/{prodex_filename}.parquet"
        )
        client.put_object(
            MINIO_BUCKET_NAME, filename, data=BytesIO(file_data), length=len(file_data), content_type="application/csv"
        )
        os.remove(prodex_json_filepath)


@dag( 
    dag_id='gas_power_gross',
    schedule=timedelta(minutes=3),
    start_date=pendulum.datetime(2023, 5, 1, 0, 0, 0, tz="Europe/Copenhagen"),
    catchup=True,
    max_active_tasks=5,
    max_active_runs=5,
    tags=['experimental', 'gas', 'rest api'],
    default_args=default_task_args,)
def gas_power_gross():
    print("Doing gas")
    setups()
    if __name__ != "__main__": # as in "normal" operation as DAG stated in Airflow
        eProdex_jsons = extract_GasProdex()
    else: # more or less test mode
        args = {
            'ts' : datetime.now().isoformat(),
            'data_interval_end' : datetime.fromisoformat("2023-6-01T01:00:00+00:00"),
            'data_interval_start' : datetime.fromisoformat("2022-6-01T01:00:00+00:00")
        }
        eProdex_jsons = extract_GasProdex(**args)
    write_to_bucket(eProdex_jsons, 'gaspower')



gas_power_gross()