from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago 
from datetime import datetime, timedelta

## Step 2: Default Args 
args = {
    'owner': 'airflow',
    'start_date': datetime(2020,10,25),
    'end_date': datetime(2020,10,30),
    'retries': 10,
    'rety_delay': timedelta(minutes=5),
    'email':'rkchatti@gmail.com',
    'email_on_failure':False,
    'email_on_retry':False
}

## Step 3: Initiate DAG 
dag = DAG('GCS_Sensor',
        default_args = args,
        schedule_interval = '0 0 * * *')

## Step 4: Define Tasks 
t1 = GoogleCloudStorageObjectSensor(
    task_id = 'sensor_task',
    bucket = 'airflow_data_in',
    object = 'test_sensor_file_in_2.txt',
    google_cloud_conn_id = 'my_gcp_conn',
    dag = dag
)

t2 = BashOperator(
    task_id = 'Bash_Task',
    bash_command = 'date',
    dag = dag
)

## Step 5: Dependencies
t1 >> t2