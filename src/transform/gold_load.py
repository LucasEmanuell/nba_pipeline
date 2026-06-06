import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_SCHEDULE_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule")
SILVER_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "silver", "boxscores")
SILVER_PLAYER_DIR = os.path.join(BASE_DIR, "data", "silver", "player_stats")

# Tenta conexão interna (Docker) primeiro; cai para externa se não configurada.
DB_URL = os.getenv("DB_URL_INTERNAL") or os.getenv("DB_URL_EXTERNAL")


def _read_delta(path: str) -> pd.DataFrame | None:
    """Lê um Delta table com deltalake (delta-rs) — sem Spark, sem overhead de JVM."""
    if not os.path.exists(path):
        return None
    try:
        return DeltaTable(path).to_pandas()
    except TableNotFoundError:
        logger.warning(f"Delta table em {path} existe mas está corrompido (delta_log vazio) — pulando")
        return None


def _ensure_schedule_schema(engine) -> None:
    """Migration idempotente: adiciona colunas novas sem destruir dados existentes."""
    new_columns = [
        "ALTER TABLE dim_nba_schedule ADD COLUMN IF NOT EXISTS game_type VARCHAR",
        "ALTER TABLE dim_nba_schedule ADD COLUMN IF NOT EXISTS home_series_wins INTEGER",
        "ALTER TABLE dim_nba_schedule ADD COLUMN IF NOT EXISTS away_series_wins INTEGER",
        "ALTER TABLE dim_nba_schedule ADD COLUMN IF NOT EXISTS poll_message_id BIGINT",
    ]
    with engine.begin() as conn:
        for sql in new_columns:
            try:
                conn.execute(text(sql))
            except ProgrammingError:
                pass  # tabela ainda não existe — será criada pelo to_sql a seguir


def _ensure_bot_execucoes(engine) -> None:
    """Cria a tabela de controle de idempotência dos bots se não existir."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bot_execucoes (
                data_execucao DATE PRIMARY KEY,
                enquetes_enviadas BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))


def _set_primary_key(engine, table_name: str, pk_column: str) -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column})"))
        except ProgrammingError:
            pass  # chave já existe


def _upsert(engine, df: pd.DataFrame, table_name: str, pk_column: str) -> int:
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as conn:
        for record in df.to_dict(orient='records'):
            stmt = insert(table).values(**record)
            update_cols = {c.name: c for c in stmt.excluded if c.name != pk_column}
            conn.execute(stmt.on_conflict_do_update(index_elements=[pk_column], set_=update_cols))

    return len(df)


def load_silver_to_gold(target_date: str) -> None:
    logger.info(f"Iniciando carga Silver → Gold para {target_date}")

    engine = create_engine(DB_URL)
    _ensure_bot_execucoes(engine)

    # ── Calendário ───────────────────────────────────────────────────────────
    schedule_path = os.path.join(SILVER_SCHEDULE_DIR, target_date)
    df_schedule = _read_delta(schedule_path)

    if df_schedule is not None:
        # to_sql com head(0) cria a tabela se não existir, sem inserir dados
        df_schedule.head(0).to_sql('dim_nba_schedule', engine, if_exists='append', index=False)
        _ensure_schedule_schema(engine)
        _set_primary_key(engine, 'dim_nba_schedule', 'game_id')

        count = _upsert(engine, df_schedule, 'dim_nba_schedule', 'game_id')
        logger.info(f"dim_nba_schedule: {count} registros upsertados para {target_date}")
    else:
        logger.warning(f"Silver Schedule não encontrado para {target_date} — pulando")

    # ── Boxscores ────────────────────────────────────────────────────────────
    boxscore_path = os.path.join(SILVER_BOXSCORE_DIR, target_date)
    df_boxscores = _read_delta(boxscore_path)

    if df_boxscores is not None:
        df_boxscores.head(0).to_sql('fact_nba_boxscores', engine, if_exists='append', index=False)
        _set_primary_key(engine, 'fact_nba_boxscores', 'game_id')

        count = _upsert(engine, df_boxscores, 'fact_nba_boxscores', 'game_id')
        logger.info(f"fact_nba_boxscores: {count} registros upsertados para {target_date}")
    else:
        logger.warning(f"Silver Boxscores não encontrado para {target_date} — pulando")

    # ── Player Stats ─────────────────────────────────────────────────────────
    player_path = os.path.join(SILVER_PLAYER_DIR, target_date)
    df_players = _read_delta(player_path)

    if df_players is not None:
        # starter vem como bool ou string "1"/"0" dependendo do jogo — normaliza para bool
        df_players = df_players.copy()
        df_players['starter'] = df_players['starter'].isin([True, 1, '1'])

        # dim_players: uma linha por jogador, atualiza se o jogador mudar de time
        df_dim_players = df_players[["player_id", "player_name", "position"]].drop_duplicates("player_id")
        df_dim_players.head(0).to_sql('dim_players', engine, if_exists='append', index=False)
        _set_primary_key(engine, 'dim_players', 'player_id')
        _upsert(engine, df_dim_players, 'dim_players', 'player_id')

        # fact_player_game_stats: PK composta (game_id, player_id)
        df_fact = df_players.drop(columns=["player_name", "position"])
        df_fact["pk"] = df_fact["game_id"].astype(str) + "_" + df_fact["player_id"].astype(str)
        df_fact = df_fact.drop_duplicates("pk")

        df_fact.head(0).to_sql('fact_player_game_stats', engine, if_exists='append', index=False)
        _set_primary_key(engine, 'fact_player_game_stats', 'pk')
        _upsert(engine, df_fact, 'fact_player_game_stats', 'pk')

        logger.info(f"player stats: {len(df_players)} linhas, {len(df_dim_players)} jogadores unicos para {target_date}")
    else:
        logger.warning(f"Silver Player Stats não encontrado para {target_date} — pulando")


if __name__ == "__main__":
    hoje = datetime.now().strftime('%Y-%m-%d')
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    load_silver_to_gold(hoje)
    load_silver_to_gold(ontem)
