from airflow import DAG 
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

"""
This DAG Requires Run time parameters to determine FROM and TO Dataset names. 

Use the following JSON format when Triggering DAG. 

{
"from_period":"2020_P10",
"to_period":"2020_P11"
}
"""

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
    schedule_interval = None,
    default_args = default_args
)

t1 = BigQueryToBigQueryOperator(
    task_id = 'BQ_CP',
    source_project_dataset_tables='{{ dag_run.conf["from_period"] }}' + '_RETAIL_METADATA.holidays_and_events',
    destination_project_dataset_table='{{ dag_run.conf["to_period"] }}' + '_RETAIL_METADATA.holidays_and_events',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    bigquery_conn_id='bigquery_default',
    dag=dag
)