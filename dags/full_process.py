"""
Student Data ETL
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks import SFTPHook, SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.oracle_operator import OracleOperator

from hsu_etl import HSU_ETL

os.environ['NLS_LANG'] = '.AL32UTF8'

today = "{{ ds }}"

default_args = {
    'start_date': datetime(2020, 4, 16, 12, 0, 0),
    'email': ['sky.mckinley@humboldt.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
   'etl_complete',
   schedule_interval='@daily',
   default_args=default_args,
   catchup=True
)

download_tables = DummyOperator(
        task_id = 'download_tables',
        dag = dag)

update_program_stack = DummyOperator(
        task_id = 'update_program_stack',
        dag = dag)

update_photos = DummyOperator(
        task_id = 'update_photos',
        dag = dag)

update_term_data = DummyOperator(
        task_id = 'update_term_data',
        dag = dag)

update_addresses = DummyOperator(
        task_id = 'update_addresses',
        dag = dag)

materialize_vip = DummyOperator(
        task_id = 'materialize_vip',
        dag = dag)

materialize_ee = DummyOperator(
    task_id = 'materialize_ee',
    dag = dag)

person_cdc = DummyOperator(
    task_id = 'person_cdc',
    dag = dag)

update_acad_org = DummyOperator(
    task_id = 'update_acad_org',
    dag = dag)

update_majors = DummyOperator(
    task_id = 'update_majors',
    dag = dag)

update_cip_and_hegis = DummyOperator(
    task_id = 'update_cip_and_hegis',
    dag = dag)

download_tables >> update_program_stack >> update_photos >> update_term_data 
update_term_data >> update_addresses >> materialize_vip >> materialize_ee
materialize_ee >> person_cdc >> update_acad_org >> update_majors 
update_majors >> update_cip_and_hegis
