"""
Housing Applications ETL
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
import os

from airflow import DAG
from airflow.contrib.hooks import SFTPHook, SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.oracle_operator import OracleOperator

from hsu_etl import HSU_ETL

os.environ['NLS_LANG'] = '.AL32UTF8'

SFTP_CONN_ID = 'sftp_bay_clover'
DB_CONN_ID = 'oie_ws'

REMOTE_FILE_PATH = '/home/prodacct/input/hsng2oie'
REMOTE_FILE_NAME = 'oie_application_stats.csv'

LOCAL_FILE_PATH = '/tmp'
LOCAL_FILE_NAME = '/housing_application_stats.csv'

today = "{{ ds }}"

default_args = {
    'start_date': datetime(2020, 1, 8, 13, 15, 0),
    'email': ['sky.mckinley@humboldt.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
   'etl_housing_applications',
   schedule_interval='@daily',
   default_args=default_args,
   catchup=True
)

def check_for_file_py(**kwargs):
    path = kwargs.get('path', None)
    sftp_conn_id = kwargs.get('sftp_conn_id', None)
    filename = kwargs.get('templates_dict').get('filename', None)
    ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
    sftp_client = ssh_hook.get_conn().open_sftp()
    sftp_files = sftp_client.listdir(path)

    if filename in sftp_files:
        return True
    else:
        return False

def bulk_load_csv(table, **kwargs):
    local_filepath = kwargs.get('local_filepath')
    oracle_conn_id = kwargs.get('oracle_conn_id')

    df = pd.read_csv(local_filepath)
    df = df.astype(str)
    
    rows = df.values.tolist()

    conn = OracleHook(oracle_conn_id=oracle_conn_id)
    conn.bulk_insert_rows(table=table, rows=rows)
    return table

filecheck = ShortCircuitOperator(
                task_id='check_for_file',
                python_callable=check_for_file_py,
                templates_dict={'filename':REMOTE_FILE_NAME},
                op_kwargs={'path':REMOTE_FILE_PATH,
                    'sftp_conn_id': SFTP_CONN_ID}, 
                provide_context=True, 
                dag=dag) 

sftp = SFTPOperator(task_id='retrieve_file',
        ssh_conn_id = SFTP_CONN_ID,
        remote_filepath=REMOTE_FILE_PATH + '/' + REMOTE_FILE_NAME,
        local_filepath=LOCAL_FILE_PATH + '/' + LOCAL_FILE_NAME,
        operation="get",
        dag=dag)

clear_extract_table = OracleOperator(
        task_id = 'clear_extract_table',
        sql = 'delete from oie_ws.extr_oie_housing_apps',
        oracle_conn_id = DB_CONN_ID,
        dag = dag)

transform = OracleOperator(
        task_id = 'transform',
        sql = "sql/hsg_apps_extr_to_stg.sql",
        oracle_conn_id = DB_CONN_ID,
        dag = dag)

extract = PythonOperator(
        task_id='extract',
        provide_context=True,
        python_callable=bulk_load_csv,
        op_kwargs={'table': 'EXTR_OIE_HOUSING_APPS',
            'oracle_conn_id':DB_CONN_ID,
            'local_filepath':LOCAL_FILE_PATH + '/' + LOCAL_FILE_NAME},
        dag=dag)

load = OracleOperator(
        task_id = 'load',
        sql = 'sql/load_app_dimensions.sql',
        oracle_conn_id = DB_CONN_ID,
        dag = dag)

sftp.set_upstream(filecheck)
extract.set_upstream(sftp)
extract.set_upstream(clear_extract_table)
transform.set_upstream(extract)
load.set_upstream(transform)
