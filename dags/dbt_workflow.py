from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from datetime import timedelta

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig, ProfileConfig
from pathlib import Path


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60*4),
    tags=['dbt workflow'],
)

def dbt_pipeline():

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/daily_transform']
        )
    )

    chain(
        transform
    )

dbt_pipeline()