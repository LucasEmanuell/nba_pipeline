import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import ProgrammingError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_SCHEDULE_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule_enriched")
SILVER_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "silver", "boxscores")

# Conexão com o PostgreSQL
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
# DB_URL = "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"

def set_primary_key(engine, table_name, pk_column):
    """Garante que a tabela tenha uma chave primária para o UPSERT funcionar no PostgreSQL."""
    with engine.begin() as conn:
        try:
            # Tenta adicionar a chave primária. 
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});"))
            logging.info(f"🔑 Chave primária ({pk_column}) adicionada na tabela {table_name}.")
        except ProgrammingError as e:
            # Se a chave já existir, o Postgres retorna erro. Nós apenas ignoramos e seguimos em frente.
            pass

def load_silver_to_gold(target_date: str):
    logging.info(f"Iniciando carga (Load) da camada Silver para a Gold (PostgreSQL) - Data: {target_date}")
    
    engine = create_engine(DB_URL)
    metadata = MetaData()
    
    schedule_path = os.path.join(SILVER_SCHEDULE_DIR, target_date)
    boxscore_path = os.path.join(SILVER_BOXSCORE_DIR, target_date)
    
    # ==========================================
    # 1. CARGA DO CALENDÁRIO
    # ==========================================
    if os.path.exists(schedule_path):
        logging.info("Lendo Parquet de Calendário (Silver)...")
        df_schedule = pd.read_parquet(schedule_path)
        
        # Cria a tabela no Postgres se não existir (apenas a estrutura, sem dados)
        df_schedule.head(0).to_sql('dim_nba_schedule', engine, if_exists='append', index=False)
        
        # Garante a Chave Primária antes de fazer o Upsert
        set_primary_key(engine, 'dim_nba_schedule', 'game_id')
        
        # Reflete a estrutura da tabela do banco de forma correta (Padrão SQLAlchemy 2.0)
        table_schedule = Table('dim_nba_schedule', metadata, autoload_with=engine)
        
        logging.info("Inserindo dados na tabela dim_nba_schedule (UPSERT)...")
        with engine.begin() as conn:
            for record in df_schedule.to_dict(orient='records'):
                stmt = insert(table_schedule).values(**record)
                
                # Se o game_id já existir, atualiza as outras colunas. Se não, insere novo.
                update_dict = {c.name: c for c in stmt.excluded if c.name != 'game_id'}
                upsert_stmt = stmt.on_conflict_do_update(index_elements=['game_id'], set_=update_dict)
                conn.execute(upsert_stmt)
                
        logging.info("✅ Calendário carregado com sucesso na Gold!")
    else:
        logging.warning("⚠️ Calendário não encontrado para essa data.")

    # ==========================================
    # 2. CARGA DOS PLACARES (BOXSCORES)
    # ==========================================
    if os.path.exists(boxscore_path):
        logging.info("Lendo Parquet de Boxscores (Silver)...")
        df_boxscores = pd.read_parquet(boxscore_path)
        
        df_boxscores.head(0).to_sql('fact_nba_boxscores', engine, if_exists='append', index=False)
        set_primary_key(engine, 'fact_nba_boxscores', 'game_id')
        
        table_boxscores = Table('fact_nba_boxscores', metadata, autoload_with=engine)
        
        logging.info("Inserindo dados na tabela fact_nba_boxscores (UPSERT)...")
        with engine.begin() as conn:
            for record in df_boxscores.to_dict(orient='records'):
                stmt = insert(table_boxscores).values(**record)
                update_dict = {c.name: c for c in stmt.excluded if c.name != 'game_id'}
                upsert_stmt = stmt.on_conflict_do_update(index_elements=['game_id'], set_=update_dict)
                conn.execute(upsert_stmt)
                
        logging.info("✅ Boxscores carregados com sucesso na Gold!")
    else:
        logging.warning("⚠️ Boxscores não encontrados para essa data.")

if __name__ == "__main__":
    # Carregando os dados de ontem e de hoje
    hoje = datetime.now().strftime('%Y-%m-%d')
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    load_silver_to_gold(hoje)
    load_silver_to_gold(ontem)