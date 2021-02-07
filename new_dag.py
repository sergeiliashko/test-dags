from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import Optional
import tempfile

from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
import time
import os
import requests

class AppslfyerToS3Operator(BaseOperator):
    template_fields = [
        'api_key',
        'from_date',
        'to_date',
    ]
    @apply_defaults
    def __init__(
            self,
            api_key: str,
            s3_conn_id: str,
            report_type: str, # installs/events
            store_id: str,
            from_date: str,
            to_date: str,
            timezone: Optional[str] = "UTC",
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.api_key = api_key
        self.s3_conn_id = s3_conn_id
        self.report_type = report_type
        self.store_id = store_id
        self.from_date = from_date
        self.to_date = to_date
        self.timezone = timezone

    def execute(self, context):
        APPSFLYER_HOST = "https://hq.appsflyer.com"
        API_VERSION = "v5"
        APPSFLYER_ENDPOINT = f"export/{self.store_id}/{self.report_type}_report/{API_VERSION}"
        data = {'api_token':self.api_key,"from":self.from_date,"to":self.to_date,"timezone":self.timezone}
        data_date = datetime.fromisoformat(self.from_date)
        with requests.get(f"{APPSFLYER_HOST}/{APPSFLYER_ENDPOINT}", params=data, stream=True) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile() as temp_file:
                for chunk in r.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                hook = S3Hook(self.s3_conn_id)
                hook.load_file(filename=temp_file.name,
                        key=f'appsflyer/{self.store_id}/{self.report_type}/{data_date.strftime("%Y")}/{data_date.strftime("%Y%m")}/{self.store_id}-{self.report_type}_{data_date.strftime("%Y_%m_%d")}.csv',
                        bucket_name="zif-spaces-1")


args = {
    'owner': 'airflow',
    "start_date": datetime(2021, 1, 1),
    'depends_on_past': False,
}
with DAG('pull_sokolov_from_trackers', schedule_interval='@daily', default_args=args, tags=['appsflyer','sokolov']) as dag:
    dag.doc_md ="### Sokolov initial data pull from appsflyer"

    task_get_op = AppslfyerToS3Operator(
        task_id='pull_ios_installs',
        api_key='{{ var.value.apikey }}',
        s3_conn_id="MyS3Conn",
        report_type="installs",
        store_id='id1501705341',
        from_date='{{ ds }}',
        to_date='{{ ds }}',
        dag=dag,
    )
    task_get_op.doc_md = """\
            #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
        """
    task_get_ios_events = AppslfyerToS3Operator(
        task_id='pull_ios_events',
        api_key='{{ var.value.apikey }}',
        s3_conn_id="MyS3Conn",
        report_type="in_app_events",
        store_id='id1501705341',
        from_date='{{ ds }}',
        to_date='{{ ds }}',
        dag=dag,
    )
    task_get_android_installs = AppslfyerToS3Operator(
        task_id='pull_android_installs',
        api_key='{{ var.value.apikey }}',
        s3_conn_id="MyS3Conn",
        report_type="installs",
        store_id='ru.sokolov.android',
        from_date='{{ ds }}',
        to_date='{{ ds }}',
        dag=dag,
    )

    task_get_android_events = AppslfyerToS3Operator(
        task_id='pull_android_events',
        api_key='{{ var.value.apikey }}',
        s3_conn_id="MyS3Conn",
        report_type="in_app_events",
        store_id='ru.sokolov.android',
        from_date='{{ ds }}',
        to_date='{{ ds }}',
        dag=dag,
    )
