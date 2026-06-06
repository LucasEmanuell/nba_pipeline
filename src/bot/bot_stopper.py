import os
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("GROUP_ID")
DB_URL = os.getenv("DB_URL_INTERNAL") or os.getenv("DB_URL_EXTERNAL")

CLOSE_BEFORE_MINUTES = 10


def _close_poll(message_id: int) -> bool:
    """Fecha a enquete no Telegram. Retorna True se fechou ou já estava fechada."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/stopPoll"
    response = requests.post(url, json={"chat_id": CHAT_ID, "message_id": message_id}, timeout=10)
    if response.ok:
        return True
    error = response.json().get('description', '')
    # Poll already closed ou mensagem não existe — ambos são estados finais aceitáveis
    if 'poll has already been closed' in error or 'message not found' in error.lower():
        return True
    logger.error(f"Erro ao fechar enquete {message_id}: {error}")
    return False


def _clear_poll_message_id(engine, game_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE dim_nba_schedule SET poll_message_id = NULL WHERE game_id = :gid"),
            {"gid": game_id},
        )


def encerrar_enquetes():
    if not all([TELEGRAM_TOKEN, CHAT_ID, DB_URL]):
        logger.error("Variáveis de ambiente ausentes")
        return

    engine = create_engine(DB_URL)
    now_utc = datetime.now(timezone.utc)

    query = text("""
        SELECT game_id, poll_message_id, game_datetime_utc
        FROM dim_nba_schedule
        WHERE poll_message_id IS NOT NULL
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.info("Nenhuma enquete ativa no momento.")
        return

    fechadas = 0
    for _, row in df.iterrows():
        game_time = pd.Timestamp(row['game_datetime_utc']).to_pydatetime()
        if game_time.tzinfo is None:
            game_time = game_time.replace(tzinfo=timezone.utc)

        close_at = game_time - timedelta(minutes=CLOSE_BEFORE_MINUTES)
        restam = (close_at - now_utc).total_seconds() / 60

        if now_utc >= close_at:
            if _close_poll(int(row['poll_message_id'])):
                _clear_poll_message_id(engine, row['game_id'])
                fechadas += 1
                logger.info(f"Enquete fechada: game_id={row['game_id']}")
        else:
            logger.info(f"game_id={row['game_id']}: fecha em {restam:.0f} min")

    if fechadas:
        logger.info(f"{fechadas} enquete(s) encerrada(s).")


if __name__ == "__main__":
    encerrar_enquetes()
