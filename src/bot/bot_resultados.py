import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("GROUP_ID")
DB_URL = os.getenv("DB_URL_INTERNAL") or os.getenv("DB_URL_EXTERNAL")


def _send_message(text_body: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    response = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text_body,
        "parse_mode": "Markdown",
    }, timeout=10)
    if not response.ok:
        logger.error(f"Falha ao enviar mensagem: {response.text}")


def _ja_enviou_resultados(engine, data_jogo: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT resultados_enviados FROM bot_execucoes WHERE data_execucao = :d"),
            {"d": data_jogo},
        ).fetchone()
    return result is not None and result[0]


def _marcar_resultados_enviados(engine, data_jogo: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bot_execucoes (data_execucao, resultados_enviados)
            VALUES (:d, TRUE)
            ON CONFLICT (data_execucao) DO UPDATE SET resultados_enviados = TRUE
        """), {"d": data_jogo})


def main():
    if not all([TELEGRAM_TOKEN, CHAT_ID, DB_URL]):
        logger.error("Variáveis de ambiente ausentes (BOT_TOKEN, GROUP_ID, DB_URL)")
        return

    engine = create_engine(DB_URL)
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    if _ja_enviou_resultados(engine, ontem):
        logger.info(f"Resultados de {ontem} já foram enviados — pulando.")
        return

    logger.info(f"Buscando resultados finalizados de {ontem}...")

    # DATE AT TIME ZONE garante que jogos que começam às 23h BRT (02h UTC do dia seguinte)
    # sejam contados como pertencentes ao dia brasileiro correto.
    query = text("""
        SELECT
            s.away_team_name, s.home_team_name,
            b.away_score, b.home_score, b.winner
        FROM dim_nba_schedule s
        JOIN fact_nba_boxscores b ON s.game_id = b.game_id
        WHERE DATE(s.game_datetime_utc AT TIME ZONE 'America/Sao_Paulo') = :ontem
          AND b.is_final = TRUE
        ORDER BY s.game_datetime_utc
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"ontem": ontem})

    if df.empty:
        logger.info("Nenhum resultado finalizado para ontem.")
        return

    linhas = []
    for _, row in df.iterrows():
        away_marker = " ✅" if row['winner'] == 'away' else ""
        home_marker = " ✅" if row['winner'] == 'home' else ""
        linhas.append(
            f"{row['away_team_name']}{away_marker} *{row['away_score']}*"
            f"  x  "
            f"*{row['home_score']}* {row['home_team_name']}{home_marker}"
        )

    mensagem = f"🏀 *Resultados NBA — {ontem}*\n\n" + "\n\n".join(linhas)
    _send_message(mensagem)
    _marcar_resultados_enviados(engine, ontem)
    logger.info(f"{len(df)} resultados enviados.")


if __name__ == "__main__":
    main()
