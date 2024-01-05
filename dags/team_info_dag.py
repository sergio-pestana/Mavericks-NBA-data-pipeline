from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime

from include.src.api_ingestion.fetch_players import fetch_and_save_players_to_csv
from include.src.api_ingestion.fetch_teams import fetch_and_save_teams_data

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['api data workflow'],
)

def team_info():

    # running the scripts to fetch the api data

    fetch_nba_teams_task = PythonOperator(
        task_id='fetch_nba_teams_task',
        python_callable=fetch_and_save_teams_data,
        op_args=['include/raw_datasets/teams.csv']
    )

    fetch_nba_players_task = PythonOperator(
        task_id='fetch_nba_players_task',
        python_callable=fetch_and_save_players_to_csv,
        op_args=['include/raw_datasets/teams.csv', 'include/raw_datasets/players.csv'],
    )
    # Save the CSV files into Google Cloud Storage

    upload_teams_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_teams_to_gcs',
        src='include/raw_datasets/teams.csv',
        dst='raw/teams.csv',
        bucket='mavericks_data',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    upload_players_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_players_to_gcs',
        src='include/raw_datasets/players.csv',
        dst='raw/players.csv',
        bucket='mavericks_data',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    # Start working on BigQuery

    teams_gcs_to_raw = aql.load_file(
        task_id='teams_gcs_to_raw',
        input_file=File(
            'gs://mavericks_data/raw/teams.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_teams',
            conn_id='gcp',
            metadata=Metadata(schema='raw')
        ),
        use_native_support=False,
    )

    players_gcs_to_raw = aql.load_file(
        task_id='players_gcs_to_raw',
        input_file=File(
            'gs://mavericks_data/raw/players.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_players',
            conn_id='gcp',
            metadata=Metadata(schema='raw')
        ),
        use_native_support=False,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_pipeline'
    )

    chain(
        fetch_nba_teams_task,
        fetch_nba_players_task,
        upload_teams_to_gcs,
        upload_players_to_gcs,
        teams_gcs_to_raw,
        players_gcs_to_raw,
        trigger_dbt
    )

team_info()