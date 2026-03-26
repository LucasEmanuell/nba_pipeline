import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("GROUP_ID")
DB_URL = os.getenv("DB_URL_EXTERNAL")

def send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        print(f"Erro ao enviar mensagem: {response.text}")

def main():
    if not all([TELEGRAM_TOKEN, CHAT_ID, DB_URL]):
        print("Erro: Variáveis de ambiente ausentes no arquivo .env!")
        return

    engine = create_engine(DB_URL)
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📊 Buscando resultados dos jogos de {ontem}...")

    query = f"""
        SELECT 
            s.away_team_name, s.home_team_name, 
            b.away_score, b.home_score
        FROM dim_nba_schedule s
        JOIN fact_nba_boxscores b ON s.game_id = b.game_id
        WHERE DATE(s.game_datetime_utc) = '{ontem}'
        AND b.is_final = TRUE
    """
    df_resultados = pd.read_sql(query, engine)

    if df_resultados.empty:
        print("Nenhum resultado finalizado encontrado para ontem.")
        return

    mensagem = f"🏀 *Resultados da NBA ({ontem})* 🏀\n\n"
    
    for _, row in df_resultados.iterrows():
        linha = f"{row['away_team_name']} {row['away_score']} x {row['home_score']} {row['home_team_name']}\n"
        mensagem += linha

    print(f"Enviando mensagem aos inscritos...\n")
    send_message(mensagem)

if __name__ == "__main__":
    main()