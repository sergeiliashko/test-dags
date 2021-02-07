from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
import time
import os

args = {
    'owner': 'airflow',
    "start_date": datetime(2021, 1, 27),
    "end_date": datetime(2021, 1, 30),
}


with DAG('download_api_file', schedule_interval='@daily', default_args=args) as dag:
    task_get_op = SimpleHttpOperator(
        task_id='get_file_sokolov',
        http_conn_id='appsflyerConn',
        method='GET',
        endpoint='export/id1501705341/installs_report/v5',
        data={'api_token': "{{ var.value.apikey }}", 'from': '{{ ds }}', 'to': '{{ ds }}', 'timezone': 'UTC'},
        log_response=True,
        dag=dag,
    )
