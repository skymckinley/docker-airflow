"""
Fetch the housing applications file export on Bay and store it in the /tmp
directory.
"""

from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.contrib.hooks import sftp_hook
from airflow.contrib.operators import sft_operator

SFTP_HOST = 'bay.humboldt.edu'
SFTP_USER = 'cloversvc'
SFTP_PWD = 'xsw21qaz'

REMOTE_FILE_PATH = '/home/prodacct/input/hsng2oie'
REMOTE_FILE_NAME = 'oie_application_stats.csv'

LOCAL_FILE_PATH = '/tmp'
LOCAL_FILE_NAME = '/housing_application_stats.csv'

today = "{{ ds }}"

default_args = {
    'start_date': datetime(2020, 1, 8, 0, 0, 0),
    'email': ['sky.mckinley@humboldt.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
   'sftp_housing_apps_file',
   schedule_interval='@daily',
   default_args=default_args,
   catchup=True
)

