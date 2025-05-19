from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models.param import Param

gcs_bucket='bigquery-projectss'
default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
    'BigQuery_Health_Records_Ingestion',
    default_args=default_args,
    description=" Big Query Health Data Records Ingestion in Big Query",
    catchup=False,
    schedule_interval=None,
    start_date=datetime(2025,5,19),
    tags=['dev'],
    params={
        'execution_date':Param(default='NA',type='string',description='Execution date in YYYYMMDD format'),
    },
)

def get_execution_date(ds_nodash,**kwargs):
    execution_date=kwargs['params'].get('execution_date','NA')
    if execution_date=='NA':
        return ds_nodash
    return execution_date

get_date_task=PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ds_nodash}}'},
    dag=dag,
)

file_sensing_task=GCSObjectExistenceSensor(
    task_id='File_sensing_for_todays_date',
    bucket=gcs_bucket,
    object='hospital/health_records_{{ task_instance.xcom_pull(task_ids="get_execution_date") }}.json',
    google_cloud_conn_id='google_cloud_default',
    timeout=300,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

pyspark_job="gs://bigquery-projectss/spark_job/spark_job.py"


CLUSTER_NAME='hadoop-cluster'
PROJECT_ID='fit-legacy-454720-g4'
REGION='us-central1'
spark_task=DataprocSubmitJobOperator(
    task_id="pyspark_job",
    region=REGION,
    project_id=PROJECT_ID,
    job={
        "placement":{"cluster_name":CLUSTER_NAME},
        "pyspark_job":{"main_python_file_uri":pyspark_job, "args":['--date={{ti.xcom_pull(task_ids="get_execution_date")}}'],
        },
    },
    dag=dag,
)

get_date_task>>file_sensing_task>>spark_task