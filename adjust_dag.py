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

class AdjustCohortToS3Operator(BaseOperator):
    template_fields = [
        'app_token',
        'user_token',
        'from_date',
        'to_date',
    ]
    @apply_defaults
    def __init__(
            self,
            s3_conn_id: str,
            app_token: str,
            user_token: str,
            from_date: str,
            to_date: str,
            kpis: str,
            event_kpis: str,
            cohort_lookback_period: int,
            timezone_offset: Optional[str] = "00:00",
            period: Optional[str] = "day", #day/week/month,
            human_readable_kpis: Optional[str] = "true",
            grouping: Optional[str] = "networks,campaigns,adgroups,creatives,countries,os_names,region,device_types,os_names,partners",
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.app_token = app_token
        self.user_token = user_token
        self.s3_conn_id = s3_conn_id
        self.from_date = from_date
        self.to_date = to_date
        self.timezone_offset = timezone_offset
        self.cohort_lookback_period = cohort_lookback_period
        self.human_readable_kpis = human_readable_kpis
        self.grouping = grouping
        self.kpis = kpis
        self.event_kpis = event_kpis
        self.period = period

    def execute(self, context):
        ADJUST_HOST = "https://api.adjust.com"
        API_VERSION = "v1"
        ADJUST_ENDPOINT = f"kpis/{API_VERSION}/{self.app_token}/cohorts.csv"
        start_data_date = datetime.fromisoformat(self.from_date)
        current_data_date = datetime.fromisoformat(self.to_date)
        days_to_look_back = self.cohort_lookback_period+1 \
            if current_data_date - start_data_date > timedelta(days=self.cohort_lookback_period) \
            else (current_data_date - start_data_date).days + 1
        for days_back in range(days_to_look_back):
            cur_target_date = (current_data_date - timedelta(days=days_back))
            data = {'user_token':self.user_token,
                    "start_date":cur_target_date.strftime("%Y-%m-%d"),
                    "end_date":cur_target_date.strftime("%Y-%m-%d"),
                    "utc_offset":self.timezone_offset,
                    "kpis":self.kpis,
                    "event_kpis":self.event_kpis,
                    "cohort_period_filter":days_back,
                    "period":self.period,
                    "grouping":self.grouping,
                    "human_readable_kpis":self.human_readable_kpis
                    }
            with requests.get(f"{ADJUST_HOST}/{ADJUST_ENDPOINT}", params=data, stream=True) as r:
                r.raise_for_status()
                with tempfile.NamedTemporaryFile() as temp_file:
                    for chunk in r.iter_content(chunk_size=8192):
                        temp_file.write(chunk)
                    hook = S3Hook(self.s3_conn_id)
                    hook.load_file(filename=temp_file.name,
                            key=f'adjust/{self.app_token}/cohorts/{cur_target_date.strftime("%Y")}/{cur_target_date.strftime("%Y%m")}/{self.app_token}-cohorts_{cur_target_date.strftime("%Y_%m_%d")}_{self.timezone_offset}+{days_back}d.csv',
                            bucket_name="zif-spaces-1")




args = {
    'owner': 'airflow',
    "start_date": datetime(2021, 2, 1),
    'depends_on_past': False,
}
with DAG('pull_beru_ios_from_trackers', schedule_interval='@daily', default_args=args, tags=['adjust','beru']) as dag:
    dag.doc_md ="### Beru initial data pull from adjust"

    task_get_ios_cohorts = AdjustCohortToS3Operator(
        dag=dag,
        task_id='pull_ios_cohorts_from_adjust',
        s3_conn_id="MyS3Conn",
        app_token='{{ var.value.adjust_beru_app_token }}',
        user_token='{{ var.value.adjust_user_token }}',
        from_date='2021-02-01',
        to_date='{{ ds }}',
        event_kpis="all_events",
        kpis="paying_users,retained_users,cohort_size",
        cohort_lookback_period=8
    )
