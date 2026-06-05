import os
import logging
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("GROUP_ID")
DB_URL = os.getenv("DB_URL_INTERNAL") or os.getenv("DB_URL_EXTERNAL")


def _ja_enviou_hoje(engine, hoje: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT enquetes_enviadas FROM bot_execucoes WHERE data_execucao = :d"),
            {"d": hoje},
        ).fetchone()
    return result is not None and result[0]


def _marcar_enviado(engine, hoje: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bot_execucoes (data_execucao, enquetes_enviadas)
            VALUES (:d, TRUE)
            ON CONFLICT (data_execucao) DO UPDATE SET enquetes_enviadas = TRUE
        """), {"d": hoje})


def _salvar_poll_message_id(engine, game_id: str, message_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE dim_nba_schedule SET poll_message_id = :mid WHERE game_id = :gid"),
            {"mid": message_id, "gid": game_id},
        )


def _build_poll_options(row: dict) -> list[str]:
    """Retorna as opções da enquete.

    Na temporada regular: apenas os nomes dos times.
    Nos playoffs: mostra o placar hipotético se cada time vencer este jogo.
    Ex: serie 3-1 → opções são "Lakers 4x1" (fecha) vs "Warriors 2x3" (reduz).
    """
    if row.get('game_type') == 'playoff' and row.get('home_series_wins') is not None:
        hw = int(row['home_series_wins'])
        aw = int(row['away_series_wins'])
        return [
            f"{row['away_team_name']} {aw + 1}x{hw}",
            f"{row['home_team_name']} {hw + 1}x{aw}",
        ]
    return [row['away_team_name'], row['home_team_name']]


def _send_poll(question: str, options: list[str]) -> int | None:
    """Envia enquete e retorna o message_id, ou None em caso de falha."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPoll"
    payload = {
        "chat_id": CHAT_ID,
        "question": question,
        "options": options,
        "is_anonymous": False,
    }
    response = requests.post(url, json=payload, timeout=10)
    if response.ok:
        return response.json()['result']['message_id']
    logger.error(f"Falha ao enviar enquete '{question}': {response.text}")
    return None


def main():
    if not all([TELEGRAM_TOKEN, CHAT_ID, DB_URL]):
        logger.error("Variáveis de ambiente ausentes (BOT_TOKEN, GROUP_ID, DB_URL)")
        return

    engine = create_engine(DB_URL)
    hoje = datetime.now().strftime('%Y-%m-%d')

    if _ja_enviou_hoje(engine, hoje):
        logger.info(f"Enquetes de {hoje} já foram enviadas — nada a fazer.")
        return

    # Filtra por data no fuso de Brasília e exclui jogos que já têm enquete ativa.
    query = text("""
        SELECT
            game_id, away_team_name, home_team_name,
            brazil_broadcaster, game_type,
            home_series_wins, away_series_wins
        FROM dim_nba_schedule
        WHERE DATE(game_datetime_utc AT TIME ZONE 'America/Sao_Paulo') = :hoje
          AND brazil_broadcaster IS NOT NULL
          AND poll_message_id IS NULL
        ORDER BY game_datetime_utc
    """)

    with engine.connect() as conn:
        df_jogos = pd.read_sql(query, conn, params={"hoje": hoje})

    if df_jogos.empty:
        logger.info("Nenhum jogo com transmissão no Brasil hoje.")
        return

    logger.info(f"{len(df_jogos)} jogos encontrados para {hoje}")

    for _, row in df_jogos.iterrows():
        pergunta = f"{row['away_team_name']} @ {row['home_team_name']} — {row['brazil_broadcaster']}"
        opcoes = _build_poll_options(row)

        message_id = _send_poll(pergunta, opcoes)
        if message_id:
            _salvar_poll_message_id(engine, row['game_id'], message_id)
            logger.info(f"Enquete enviada: {pergunta} (message_id={message_id})")

    _marcar_enviado(engine, hoje)
    logger.info("Todas as enquetes enviadas e execução registrada.")


if __name__ == "__main__":
    main()
