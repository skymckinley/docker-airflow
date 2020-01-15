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

def get_dim_data_from_stg(**kwargs):
    oracle_conn_id = kwargs.get('oracle_conn_id')

    conn = OracleHook(oracle_conn_id=oracle_conn_id)

    # Create an instance of the ETL utility class.
    etl = HSU_ETL()

    # SQL to read the staging table data.
    sql = """
        select 
            a.housing_app_key housing_app_key,
            b.application_id application_id,
            b.emplid emplid,
            b.applicant_name applicant_name,
            b.residence_session residence_session,
            b.student_category student_category,
            b.application_status application_status,
            b.data_origin data_origin,
            nvl(a.created_ew_dttm, systimestamp) created_ew_dttm,
            systimestamp lastupd_ew_dttm,
            'Y' current_ind,
            nvl(a.eff_start_dt, trunc(sysdate)) eff_start_dt,
            to_date('2199-12-31', 'YYYY-MM-DD') eff_end_dt,
            null hash,
            '-' src_sys_ind,
            b.loaded loaded,
            b.rowid row_id
        from stg_oie_housing_apps b
        left outer join dim_oie_housing_application a
        on a.application_id = b.application_id
        and a.current_ind = 'Y'
        where b.loaded = 'N'
        order by b.app_rcvd_dttm
        """

    stg_df = conn.get_pandas_df(sql=sql)

    stg_df['HASH'] = stg_df.apply(lambda row: etl.hash(
        row['APPLICATION_ID'],
        row['EMPLID'], 
        row['APPLICANT_NAME'],
        row['RESIDENCE_SESSION'],
        row['STUDENT_CATEGORY'], 
        row['APPLICATION_STATUS']
        ), axis=1)

    merge_sql = """
        merge into dim_oie_housing_application a
        using (
            select 
                :1 housing_app_key, 
                :2 application_id,
                :3 emplid,
                :4 applicant_name,
                :5 residence_session,
                :6 student_category,
                :7 application_status,
                :8 data_origin,
                :9 created_ew_dttm,
                :10 lastupd_ew_dttm, 
                :11 current_ind,
                :12 eff_start_dt,
                :13 eff_end_dt,
                :14 hash,
                :15 src_sys_ind
            from dual
        ) t
        on (a.application_id = t.application_id)
        when matched then update
            set
                a.current_ind = 'N',
                a.eff_end_dt = trunc(sysdate)
            where a.hash <> t.hash
        when not matched then insert
            (housing_app_key, application_id, emplid, applicant_name, 
            residence_session, student_category, application_status,
            data_origin, created_ew_dttm, lastupd_ew_dttm, current_ind,
            eff_start_dt, eff_end_dt, hash, src_sys_ind)
            values
            (
                t.housing_app_key,
                t.application_id,
                t.emplid,
                t.applicant_name,
                t.residence_session,
                t.student_category,
                t.application_status,
                t.data_origin,
                t.created_ew_dttm,
                t.lastupd_ew_dttm,
                t.current_ind,
                t.eff_start_dt,
                t.eff_end_dt,
                t.hash,
                t.src_sys_ind)
        """

    stg_df.apply(lambda row: conn.run(sql=merge_sql, parameters=(
        row['HOUSING_APP_KEY'],
        row['APPLICATION_ID'],
        row['EMPLID'],
        row['APPLICANT_NAME'],
        row['RESIDENCE_SESSION'],
        row['STUDENT_CATEGORY'],
        row['APPLICATION_STATUS'],
        row['DATA_ORIGIN'],
        row['CREATED_EW_DTTM'],
        row['LASTUPD_EW_DTTM'],
        row['CURRENT_IND'],
        row['EFF_START_DT'],
        row['EFF_END_DT'],
        row['HASH'],
        row['SRC_SYS_IND'])), axis=1)

def get_key(**kwargs):
    table_name = kwargs.get['table_name']
    key_fields = kwargs.get['key_fields']

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

get_dim_data_from_stg = PythonOperator(
        task_id = 'get_dim_data_from_stg',
        provide_context = True,
        python_callable = get_dim_data_from_stg,
        op_kwargs = {'oracle_conn_id': DB_CONN_ID},
        dag = dag)

sftp.set_upstream(filecheck)
extract.set_upstream(sftp)
extract.set_upstream(clear_extract_table)
transform.set_upstream(extract)
get_dim_data_from_stg.set_upstream(transform)
