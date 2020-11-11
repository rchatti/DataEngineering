#1 Import Libraries
from airflow import DAG 
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator

from datetime import datetime

# Default Args 
default_args = {
    'owner':'Production Operations',
    'start_date':datetime(2020,10,1),
    'end_date':None,
    'retries':1,
    'email':None,
    'email_on_retry':True,
    'email_ion_failure':False
}

# Initiate DAG 
dag = DAG('Sprouts_Check_UPCs_to_Remove', schedule_interval=None,default_args = default_args)

# T1: Wait for File on GCS 
t1 = GoogleCloudStorageObjectSensor(
    task_id = 'sensor_task',
    bucket = 'spins-tmp-ext/home/rchatti',
    object = 'sprouts_list_of_upcs.csv',
    google_cloud_conn_id = 'bigquery_default',
    dag = dag
)

# T2: Run PythonOperator to Load data to trigger sprouts_load_upc.py

def load_upc_in_bq():
    # Construct a BigQuery client object.
    project = "shining-landing-763"
    source_uri = "gs://spins-tmp-ext/home/rchatti/sprouts_list_of_upcs.csv"
    dest_dataset = "TEMP_OPS"
    dest_table = "SPROUTS_UPC_TO_DELETE_UPC_LIST"


    client = bigquery.Client()
    table_id = "{0}.{1}.{2}".format(project, dest_dataset, dest_table)

    schema = [
        bigquery.SchemaField("UPC", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema = schema
    )

    uri = source_uri
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


t2 = PythonOperator(
    task_id = "Load_UPC_to_BQ",
    python_callable = load_upc_in_bq,
    dag = dag
)


# T3: Run Query, Load data to BQ Table
load_sql = """
            SELECT
            UPC, RETLRGROUP, sum(CNT) as CNT, sum(SUM_DOLLARS) as DOLLARS_52WK
            FROM
            (
                SELECT
                CASE WHEN t3.RETAIL_TAG = 421 THEN 'Sprouts-421' ELSE 'Non-Sprouts-Retailers' END AS RETLRGROUP,
                t3.RETAIL_TAG, t3.RETAIL_CHAIN, upclist.UPC, count(*) as CNT, sum(t1.DOLLARS) as SUM_DOLLARS
                FROM `TEMP_OPS.SPROUTS_UPC_TO_DELETE_UPC_LIST` upclist

                LEFT JOIN `shining-landing-763.STANDARD_SLD_RAW.weekly_facts_agg` t1
                ON upclist.UPC = t1.UPC

                LEFT JOIN `shining-landing-763.STANDARD_SLD_RAW.dim_retailers` t3
                ON t1.RETAIL_TAG = t3.RETAIL_TAG

                and t1.partition_date > date(2018,6,1)
                and t1.AGG_PERIOD = '52 WEEKS'
                group by t3.RETAIL_TAG, t3.RETAIL_CHAIN, upclist.UPC
            ) q1
            GROUP BY UPC, RETLRGROUP
    """

project = "shining-landing-763"
dest_dataset = "TEMP_OPS"
dest_table = "SPROUTS_UPC_TO_DELETE_RESULTS"
        
t3 = BigQueryOperator(
    task_id = "Load_UPC_SALES_to_BQ",
    sql = load_sql,
    bigquery_conn_id='bigquery_default',
    destination_dataset_table = "{0}.{1}.{2}".format(project, dest_dataset, dest_table),
    create_disposition='CREATE_IF_NEEDED',
    use_legacy_sql=False,
    dag = dag
)

# T4: Airflow Copy BQ to GCS
project = "shining-landing-763"
dest_dataset = "TEMP_OPS"
dest_table = "SPROUTS_UPC_TO_DELETE_RESULTS"

t4 = BigQueryToCloudStorageOperator(
    task_id = "Load_UPC_SALES_to_GCS",
    source_project_dataset_table = "{}.{}.{}".format(project, dest_dataset, dest_table),
    destination_cloud_storage_uris = "gs://spins-tmp-ext/home/rchatti/sprouts_upc_sales.csv",
    export_format='CSV', field_delimiter=',', print_header=True,
    bigquery_conn_id='bigquery_default',
    dag = dag
)

#T5: Clean up UPC CSV
t5 = BashOperator(
    task_id = 'Delete_Processed_UPC_List',
    bash_command = 'gsutil rm gs://spins-tmp-ext/home/rchatti/sprouts_list_of_upcs.csv',
    dag = dag
)

## Dependencies
t1 >> t2 >> t3 >> t4 >> t5