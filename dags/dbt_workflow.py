from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from datetime import timedelta

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig, ProfileConfig
from pathlib import Path