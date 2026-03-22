from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importando os nossos scripts (O Docker mapeou a pasta src para dentro do Airflow)
from src.extract.get_nba import extract_schedule_to_datalake
from src.extract.scraper_jumper import extract_jumper_to_datalake
from src.extract.extract_boxscores import extract_boxscores_to_datalake
from src.transform.silver_schedule import transform_schedule_bronze_to_silver
from src.transform.silver_schedule_enriched import enrich_schedule_with_brazil_tv
from src.transform.silver_boxscores import transform_boxscores_bronze_to_silver
from src.transform.gold_load import load_silver_to_gold

# Wrappers para lidar com as datas relativas dinamicamente
def run_extract_boxscores():
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    extract_boxscores_to_datalake(ontem)

def run_transform_boxscores():
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_boxscores_bronze_to_silver(ontem)

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
    description='Pipeline de extração, transformação e carga da NBA e Jumper Brasil',
    schedule_interval='0 5 * * *', # Roda todos os dias às 05:00 da manhã
    catchup=False,
    tags=['nba', 'etl', 'pyspark'],
) as dag:

    # 1. Tasks de Extração (Camada Bronze) - Podem rodar em paralelo!
    task_extract_schedule = PythonOperator(
        task_id='extract_nba_schedule',
        python_callable=extract_schedule_to_datalake
    )

    task_extract_jumper = PythonOperator(
        task_id='extract_jumper_brasil',
        python_callable=extract_jumper_to_datalake
    )

    task_extract_boxscores = PythonOperator(
        task_id='extract_nba_boxscores',
        python_callable=run_extract_boxscores
    )

    # 2. Tasks de Transformação (Camada Silver)
    task_transform_schedule = PythonOperator(
        task_id='transform_schedule_silver',
        python_callable=transform_schedule_bronze_to_silver
    )

    task_enrich_schedule = PythonOperator(
        task_id='enrich_schedule_brazil_tv',
        python_callable=enrich_schedule_with_brazil_tv
    )

    task_transform_boxscores = PythonOperator(
        task_id='transform_boxscores_silver',
        python_callable=run_transform_boxscores
    )

    # 3. Task de Carga (Camada Gold)
    task_load_gold = PythonOperator(
        task_id='load_silver_to_gold_postgres',
        python_callable=run_gold_load
    )

    # ==========================================
    # DEFINIÇÃO DAS DEPENDÊNCIAS (A ORDEM DO FLUXO)
    # ==========================================
    
    # Fluxo do Calendário (O Enriquecimento precisa que a extração da NBA, do Jumper e a Silver da NBA estejam prontas)
    task_extract_schedule >> task_transform_schedule
    [task_transform_schedule, task_extract_jumper] >> task_enrich_schedule >> task_load_gold
    
    # Fluxo dos Placares
    task_extract_boxscores >> task_transform_boxscores >> task_load_gold