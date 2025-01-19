from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from datetime import timedelta

with DAG(
   dag_id="storing_data_from_api",
   start_date=datetime(2025,1,15),
   schedule_interval=timedelta(seconds=1),
   catchup=False
)as dag:
    @task
    def create_table():
        #initialize a postgres hook which helps to interact with postgres server
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data(
        id  SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(50)
         )
"""
        postgres_hook.run(create_table_query)
     #extractingdata
   
    extract_apod=SimpleHttpOperator(
            task_id='extract_apod',
            http_conn_id='nasa_api',
            endpoint='planetary/apod',
            method='GET',
            data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"},
            response_filter=lambda response:response.json(),
            log_response=True
        )
    
       

    @task
    def transform_data(response):
        apod_data={
            'title':response.get('title',''),
            'explanation':response.get('explanation',''),
            'url':response.get('url',''),
            'date':response.get('date',''),
            'media_type':response.get('media_type','')
            }
        return apod_data
    
    @task
    def load_data(apod_data):
        postgreshook=PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query="""
       INSERT INTO APOD_DATA(TITLE,EXPLANATION,URL,DATE,MEDIA_TYPE)
       VALUES(%s,%s,%s,%s,%s)
"""
        postgreshook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    
    create_table()>>extract_apod
    api_response=extract_apod.output
    transformed_data=transform_data(api_response)

    load_data(transformed_data)
  

