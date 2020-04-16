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
   'etl_student_data',
   schedule_interval='@daily',
   default_args=default_args,
   catchup=True
)

download_students = DummyOperator(
        task_id = 'download_students',
        dag = dag)

download_enrollment = DummyOperator(
        task_id = 'download_enrollment',
        dag = dag)

load_students = DummyOperator(
        task_id = 'load_students',
        dag = dag)

load_enrollment = DummyOperator(
        task_id = 'load_enrollment',
        dag = dag)

load_students.set_upstream(download_students)
load_enrollment.set_upstream(download_enrollment)
load_enrollment.set_upstream(load_students)
