from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime
from datetime import timedelta

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

# from cosmos.airflow.task_group import DbtTaskGroup
# from cosmos.constants import LoadMode
# from cosmos.config import ProjectConfig, RenderConfig, ProfileConfig
# from pathlib import Path

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60*4),
    tags=['cloud workflow'],
)

def cloud_pipeline():
    # Save the CSV files into Google Cloud Storage

    upload_games_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_games_to_gcs',
        src='include/raw_datasets/games.csv',
        dst='raw/games.csv',
        bucket='mavericks_data',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    upload_players_game_stats_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_players_game_stats_to_gcs',
        src='include/raw_datasets/players_game_stats.csv',
        dst='raw/players_game_stats.csv',
        bucket='mavericks_data',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    upload_total_game_stats_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_total_game_stats_to_gcs',
        src='include/raw_datasets/total_game_stats.csv',
        dst='raw/total_game_stats.csv',
        bucket='mavericks_data',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    # Start working on BigQuery

    games_gcs_to_bronze = aql.load_file(
        task_id='games_gcs_to_bronze',
        input_file=File(
            'gs://mavericks_data/raw/games.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_games',
            conn_id='gcp',
            metadata=Metadata(schema='bronze')
        ),
        use_native_support=False,
    )

    players_game_stats_gcs_to_bronze = aql.load_file(
        task_id='players_game_stats_gcs_to_bronze',
        input_file=File(
            'gs://mavericks_data/raw/players_game_stats.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_players_game_stats',
            conn_id='gcp',
            metadata=Metadata(schema='bronze')
        ),
        use_native_support=False,
    )

    total_game_stats_gcs_to_bronze = aql.load_file(
        task_id='total_game_stats_gcs_to_bronze',
        input_file=File(
            'gs://mavericks_data/raw/total_game_stats.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_total_game_stats',
            conn_id='gcp',
            metadata=Metadata(schema='bronze')
        ),
        use_native_support=False,
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath, data_source='raw')
    
    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_pipeline'
    )

    chain(
        upload_games_to_gcs,
        upload_players_game_stats_to_gcs,
        upload_total_game_stats_to_gcs,
        games_gcs_to_bronze,
        players_game_stats_gcs_to_bronze,
        total_game_stats_gcs_to_bronze,
        check_load(),
        trigger_dbt
    )

cloud_pipeline()