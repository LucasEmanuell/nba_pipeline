import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.extract.extract_boxscores_backfill import run_backfill
from src.transform.silver_boxscores import transform_boxscores_bronze_to_silver
from src.transform.silver_player_stats import transform_player_stats_bronze_to_silver
from src.transform.gold_load import load_silver_to_gold

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")
SILVER_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "silver", "boxscores")
SILVER_PLAYER_DIR = os.path.join(BASE_DIR, "data", "silver", "player_stats")


def run_silver_all_dates():
    """Processa boxscores e player stats para cada data que tem Bronze mas nao tem Silver."""
    if not os.path.exists(BRONZE_BOXSCORE_DIR):
        return

    dates_boxscores = [
        d for d in sorted(os.listdir(BRONZE_BOXSCORE_DIR))
        if not os.path.exists(os.path.join(SILVER_BOXSCORE_DIR, d))
    ]
    dates_players = [
        d for d in sorted(os.listdir(BRONZE_BOXSCORE_DIR))
        if not os.path.exists(os.path.join(SILVER_PLAYER_DIR, d))
    ]
    dates_to_process = sorted(set(dates_boxscores) | set(dates_players))

    if not dates_to_process:
        return

    # stop_spark=False reutiliza a sessao entre datas sem reiniciar a JVM a cada iteracao
    for date_str in dates_to_process:
        if date_str in dates_boxscores:
            transform_boxscores_bronze_to_silver(date_str, stop_spark=False)
        if date_str in dates_players:
            transform_player_stats_bronze_to_silver(date_str, stop_spark=False)

    from pyspark.sql import SparkSession
    active = SparkSession.getActiveSession()
    if active:
        active.stop()


def run_gold_all_dates():
    """Carrega o Gold para cada data que tem Silver de boxscores."""
    if not os.path.exists(SILVER_BOXSCORE_DIR):
        return

    for date_str in sorted(os.listdir(SILVER_BOXSCORE_DIR)):
        load_silver_to_gold(date_str)


default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 20),
    'retries': 0,
}

with DAG(
    'nba_backfill_current_season',
    default_args=default_args,
    description='Backfill de boxscores da temporada atual. Trigger manual.',
    schedule_interval=None,  # so roda quando disparado manualmente
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'backfill'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_all_boxscores_bronze',
        python_callable=run_backfill,
    )

    task_silver = PythonOperator(
        task_id='transform_all_boxscores_silver',
        python_callable=run_silver_all_dates,
    )

    task_gold = PythonOperator(
        task_id='load_all_boxscores_gold',
        python_callable=run_gold_all_dates,
    )

    task_extract >> task_silver >> task_gold
