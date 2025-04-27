from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from ELTv1 import extract_data1, extract_data2, load_data, transform_data
from datetime import timedelta, datetime


default_args = {
    'owner' : 'Rosa',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id='elt_house_pipeline',
    default_args=default_args,
    description='ELT web housing',
    schedule='@weekly',
    start_date=datetime(2025, 4, 10),
    catchup=False,
    tags=['webscraping','bs4', 'elt', 'bigquery']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data2
    )
    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_data
    )
    transform_task = BigQueryInsertJobOperator(
            task_id='transform_houses_data',
            configuration={
                    'query':{
                        'query':transform_data(),
                        'useLegacySql':False,
                    }
            },
            location='US',
            project_id='yourproject-id'
            )
        
    
    extract_task >> load_task >> transform_task

