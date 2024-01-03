from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from datetime import timedelta

from include.src.api_ingestion.fetch_games import fetch_and_save_games_data
from include.src.api_ingestion.fetch_game_stats import fetch_and_update_game_stats 
# from include.src.api_ingestion.fetch_players import fetch_and_save_players_data
# from include.src.api_ingestion.fetch_teams import fetch_and_save_teams_data

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='0 9 * * *',
    catchup=False,
    tags=['games'],
)


def games():

    fetch_games_task = PythonOperator(
        task_id='fetch_games_task',
        python_callable=fetch_and_save_games_data,
        op_args=['include/raw_datasets/games.csv'],
    )

    fetch_game_stats_task = PythonOperator(
        task_id='fetch_game_stats_task',
        python_callable=fetch_and_update_game_stats,
        op_args=['include/raw_datasets/games.csv', 'include/raw_datasets/games_stats.csv'],
    )

    chain(
        fetch_games_task,
        fetch_game_stats_task
          )


games()