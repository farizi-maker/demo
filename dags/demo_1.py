import json
import glob
import time
import ast
import datetime as dt
import numpy as np
import pandas as pd
import pandas_gbq
from pandas.io.json import json_normalize
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.oauth2 import service_account
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

gcp_hook = GoogleCloudBaseHook(gcp_conn_id="bigquery_default")
keyfile_dict = gcp_hook._get_field('keyfile_dict')
keyfile_dict = ast.literal_eval(keyfile_dict)

credentials = service_account.Credentials.from_service_account_info(keyfile_dict)
#For all json

def set_path(directory_path):
    
    #Adding all the file names; 
    file_path = []
    for file in glob.glob(directory_path):
        file_path.append(file)
        
    return file_path

def set_file(file_path):

    #Loading all the JSON files using file names
    json_file = []
    for ind in range(len(file_path)):
        with open(file_path[ind]) as f:
            json_file.append(json.load(f))
            
    return json_file

def convert_file(json_file):

    #Converting the JSON files to Data Frames
    df = pd.DataFrame()
    for ind in range(len(json_file)):
        df = df.append(pd.json_normalize(json_file[ind]))
    
    return df

def transform_nan(df):
    if df == "nan":
        return None
    else:
        return df

def change_date(df):
    if str(df) == '0000-00-00 00:00:00' or str(df) == '0000-00-00 00:00:00.000':
        return pd.NaT
    else:
        return df

def transform_account():
    directory_path = '/home/izi-99/demo/data/savings_accounts/*.json'    
    file_path = set_path(directory_path)
    json_file = set_file(file_path)
    data = convert_file(json_file)
    data = data.applymap(transform_nan)
    data = data.applymap(change_date)
    
    return data

def etl():
    data = transform_account()
    print(data)
    print('start ingesting data')
    start = time.time()

    for col in list(data.columns):
        data[col] = data[col].apply(
            lambda x: x.replace(u'\r', u' ') if isinstance(x, str) or isinstance(x, str) else x)

    data['id'] = data['id'].astype(str)
    data['op'] = data['op'].astype(str)
    data['ts'] =  pd.to_datetime(data['ts'],unit='ms')
    data['set.balance'] = data['set.balance'].astype(float)
    data['set.interest_rate_percent'] = data['set.interest_rate_percent'].astype(float)
    data['data.savings_account_id'] = data['data.savings_account_id'].astype(str)
    data['data.balance'] = data['data.balance'].astype(float)
    data['data.interest_rate_percent'] = data['data.interest_rate_percent'].astype(float)
    data['data.status'] = data['data.status'].astype(str)

    data.rename(columns={'set.balance':'set_balance', 
                                'set.interest_rate_percent':'set_interest_rate_percent',
                                'data.savings_account_id':'data_savings_account_id',
                                'data.balance':'data_balance',
                                'data.interest_rate_percent':'data_interest_rate_percent',
                                'data.status':'data_status'}, inplace=True)
 
    table_schema = [{"name": "id", "type": "STRING"},
                    {"name": "op", "type": "STRING"},
                    {"name": "ts", "type": "TIMESTAMP"},
                    {"name": "set.balance", "type": "INTEGER"},
                    {"name": "set.interest_rate_percent", "type": "INTEGER"},
                    {"name": "data.savings_account_id", "type": "STRING"},
                    {"name": "data.balance", "type": "INTEGER"},
                    {"name": "data.interest_rate_percent", "type": "INTEGER"},
                    {"name": "data.status", "type": "STRING"}]
    

    print("finish transforming data")
    print(time.time() - start, 'seconds')

    print("start inserting to BQ")
    start = time.time()
    pandas_gbq.to_gbq(data,
                    #   project_id=config.PROJECT_ID,
                    #   destination_table=config.DESTINATION_TABLE,
                      project_id='charming-storm-328004',
                      destination_table='demo.df_savings',
                      chunksize=20000,
                      table_schema=table_schema,
                      if_exists='replace',
                      credentials=credentials)
    print("finish inserting to BQ")
    print(time.time() - start, 'seconds')

default_args = {
    'owner': 'izi-demo',
    'start_date': dt.datetime(2021, 11, 3),
    'concurrency': 0,
    'retries': 0
}

dag = DAG('demo_1',
          catchup=False,
          default_args=default_args,
          schedule_interval='1 17 * * *')

opr_start = BashOperator(task_id='start_etl',
                         bash_command='echo "starting etl"',
                         dag=dag)

opr_etl = PythonOperator(task_id='etl',
                         python_callable=etl,
                         dag=dag)

opr_finish = BashOperator(task_id='finish_etl',
                          bash_command='echo "stopping etl"',
                          dag=dag)

opr_start >> opr_etl  >> opr_finish