from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.bot.bot_stopper import encerrar_enquetes

default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 20),
    'retries': 0,  # stopper é best-effort, falha não deve acumular retries
}

with DAG(
    'nba_polls_stopper',
    default_args=default_args,
    description='Encerra enquetes do Telegram 10 minutos antes de cada jogo',
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,  # evita sobreposição se uma execução demorar mais de 10 min
    tags=['nba', 'bot', 'telegram'],
) as dag:

    PythonOperator(
        task_id='stop_polls',
        python_callable=encerrar_enquetes,
    )
