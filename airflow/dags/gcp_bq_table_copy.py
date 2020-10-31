from airflow import DAG 
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

from datetime import datetime, timedelta

default_args = {
    'owner':'Prod_Ops_Team',
    'start_date':datetime(2020,10,29),
    'end_date':None,
    'email':None, 
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1
}

dag = DAG(
    'Period_Release_MIS_Copy_BQ_Holidays_Table',
    schedule_interval = '0 18 * * *',
    default_args = default_args
)

t1 = BigQueryToBigQueryOperator(
    task_id = 'BQ_CP',
    source_project_dataset_tables='airflow-learning-10262020:chicago_taxi_airflow_in.daily_table_20200930',
    destination_project_dataset_table='airflow-learning-10262020:chicago_taxi_airflow_in.daily_table_20200930_20201030',
    write_disposition='WRITE_EMPTY',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='my_gcp_conn',
    dag=dag
)