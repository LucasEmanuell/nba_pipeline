import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("GROUP_ID")
DB_URL = os.getenv("DB_URL_EXTERNAL")

def send_poll(question, options):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPoll"
    payload = {
        "chat_id": CHAT_ID,
        "question": question,
        "options": options,
        "is_anonymous": False # Garante que os votos não sejam anônimos no UI
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        print(f"Erro ao enviar enquete: {response.text}")

def main():
    if not all([TELEGRAM_TOKEN, CHAT_ID, DB_URL]):
        print("Erro: Variáveis de ambiente ausentes no arquivo .env!")
        return

    engine = create_engine(DB_URL)
    hoje = datetime.now().strftime('%Y-%m-%d')
    
    print(f"🏀 Buscando jogos da NBA com transmissão no Brasil para hoje ({hoje})...")
    
    query = f"""
        SELECT away_team_name, home_team_name, brazil_broadcaster
        FROM dim_nba_schedule
        WHERE DATE(game_datetime_utc) = '{hoje}'
        AND brazil_broadcaster IS NOT NULL
    """
    df_jogos = pd.read_sql(query, engine)

    if df_jogos.empty:
        print("Nenhum jogo com transmissão no Brasil hoje.")
        return

    for _, row in df_jogos.iterrows():
        pergunta = f"{row['away_team_name']} @ {row['home_team_name']} - Onde assistir: {row['brazil_broadcaster']}"
        opcoes = [row['away_team_name'], row['home_team_name']]
        
        print(f"Enviando enquete: {pergunta}")
        send_poll(pergunta, opcoes)

if __name__ == "__main__":
    main()