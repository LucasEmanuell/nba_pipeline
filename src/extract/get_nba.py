import requests
import json
import os
import logging
from datetime import datetime

# Configuração profissional de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")

def extract_schedule_to_datalake():
    logging.info("Iniciando extração do calendário da NBA...")
    
    try:
        response = requests.get(URL, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logging.error(f"Erro ao baixar calendário: {e}")
        return

    data_extracao = datetime.now().strftime('%Y-%m-%d')
    target_dir = os.path.join(BRONZE_DIR, data_extracao)
    os.makedirs(target_dir, exist_ok=True)
    
    file_path = os.path.join(target_dir, "schedule_league_raw.json")
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    logging.info(f"Ingestão Bronze concluída. Arquivo salvo em: {file_path}")

if __name__ == "__main__":
    extract_schedule_to_datalake()