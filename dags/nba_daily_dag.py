from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.extract.get_nba import extract_schedule_to_datalake
from src.extract.scraper_jumper import extract_jumper_to_datalake
from src.extract.extract_boxscores import extract_boxscores_to_datalake
from src.transform.silver_schedule import transform_schedule_bronze_to_silver
from src.transform.silver_jumper import transform_jumper_bronze_to_silver
from src.transform.silver_schedule_enriched import enrich_schedule_with_brazil_tv
from src.transform.silver_boxscores import transform_boxscores_bronze_to_silver
from src.transform.silver_player_stats import transform_player_stats_bronze_to_silver
from src.transform.gold_load import load_silver_to_gold
from src.bot.bot_resultados import main as send_results
from src.bot.bot_enquetes import main as send_polls


def run_extract_boxscores():
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    extract_boxscores_to_datalake(ontem)


def run_transform_boxscores():
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_boxscores_bronze_to_silver(ontem)


def run_transform_player_stats():
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_player_stats_bronze_to_silver(ontem)


def run_gold_load():
    hoje = datetime.now().strftime('%Y-%m-%d')
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    load_silver_to_gold(hoje)
    load_silver_to_gold(ontem)


default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nba_etl_daily_pipeline',
    default_args=default_args,
    description='Pipeline diário NBA: Bronze → Silver (Delta) → Gold → Telegram',
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['nba', 'etl', 'pyspark', 'delta'],
) as dag:

    # ── Extração (Bronze) ────────────────────────────────────────────────────
    task_extract_schedule = PythonOperator(
        task_id='extract_nba_schedule',
        python_callable=extract_schedule_to_datalake,
    )
    task_extract_jumper = PythonOperator(
        task_id='extract_jumper_brasil',
        python_callable=extract_jumper_to_datalake,
    )
    task_extract_boxscores = PythonOperator(
        task_id='extract_nba_boxscores',
        python_callable=run_extract_boxscores,
    )

    # ── Transformação (Silver / Delta) ───────────────────────────────────────
    task_silver_schedule = PythonOperator(
        task_id='transform_schedule_silver',
        python_callable=transform_schedule_bronze_to_silver,
    )
    task_silver_jumper = PythonOperator(
        task_id='transform_jumper_silver',
        python_callable=transform_jumper_bronze_to_silver,
    )
    task_enrich_schedule = PythonOperator(
        task_id='enrich_schedule_brazil_tv',
        python_callable=enrich_schedule_with_brazil_tv,
    )
    task_silver_boxscores = PythonOperator(
        task_id='transform_boxscores_silver',
        python_callable=run_transform_boxscores,
    )
    task_silver_player_stats = PythonOperator(
        task_id='transform_player_stats_silver',
        python_callable=run_transform_player_stats,
    )

    # ── Carga (Gold / PostgreSQL) ────────────────────────────────────────────
    task_load_gold = PythonOperator(
        task_id='load_silver_to_gold_postgres',
        python_callable=run_gold_load,
    )

    # ── Entrega (Telegram) ───────────────────────────────────────────────────
    task_send_results = PythonOperator(
        task_id='send_results_telegram',
        python_callable=send_results,
    )
    task_send_polls = PythonOperator(
        task_id='send_polls_telegram',
        python_callable=send_polls,
    )

    # ── Dependências ─────────────────────────────────────────────────────────
    #
    # Fluxo do calendário:
    #   extract_schedule ──► silver_schedule ──┐
    #                                          ├──► enrich ──► gold ──► results ──► polls
    #   extract_jumper ───► silver_jumper ─────┘
    #
    # Fluxo dos boxscores (parallel ao calendário):
    #   extract_boxscores ──► silver_boxscores ──► gold
    #
    task_extract_schedule >> task_silver_schedule
    task_extract_jumper >> task_silver_jumper
    [task_silver_schedule, task_silver_jumper] >> task_enrich_schedule >> task_load_gold
    task_extract_boxscores >> task_silver_boxscores >> task_load_gold
    task_extract_boxscores >> task_silver_player_stats >> task_load_gold
    task_load_gold >> task_send_results >> task_send_polls
