import requests
import json
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOX_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_SCHEDULE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")
BRONZE_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")

def get_game_ids_for_date(target_date: str) -> list:
    """Lê o calendário já salvo no Data Lake e retorna os IDs dos jogos de uma data específica."""
    
    # Busca no calendário extraído hoje 
    hoje_str = datetime.now().strftime('%Y-%m-%d')
    schedule_path = os.path.join(BRONZE_SCHEDULE_DIR, hoje_str, "schedule_league_raw.json")
    
    if not os.path.exists(schedule_path):
        logging.error(f"Calendário não encontrado em {schedule_path}. Execute o get_nba.py primeiro.")
        return []

    with open(schedule_path, "r", encoding="utf-8") as f:
        schedule_data = json.load(f)

    game_ids = []
    # Navega pelo JSON para achar os jogos do target_date
    for dia in schedule_data.get("leagueSchedule", {}).get("gameDates", []):
        # A API traz a data no formato "MM/DD/YYYY 00:00:00", vamos formatar ou usar substring
        game_date_str = dia.get("gameDate", "")
        
        # Pega só os primeiros 10 caracteres e converte formato para comparar (YYYY-MM-DD)
        try:
            dt_obj = datetime.strptime(game_date_str.split(" ")[0], "%m/%d/%Y")
            formatted_date = dt_obj.strftime("%Y-%m-%d")
            
            if formatted_date == target_date:
                for jogo in dia.get("games", []):
                    game_ids.append(jogo.get("gameId"))
                break # Achou a data, não precisa continuar procurando
        except Exception as e:
            logging.warning(f"Erro ao parsear data {game_date_str}: {e}")

    return game_ids

def extract_boxscores_to_datalake(target_date: str):
    logging.info(f"Buscando Game IDs para a data: {target_date}...")
    game_ids = get_game_ids_for_date(target_date)
    
    if not game_ids:
        logging.warning(f"Nenhum jogo encontrado para {target_date}.")
        return

    logging.info(f"Encontrados {len(game_ids)} jogos. Iniciando download dos boxscores...")
    
    # Cria a pasta de destino: data/bronze/boxscores/YYYY-MM-DD/
    target_dir = os.path.join(BRONZE_BOXSCORE_DIR, target_date)
    os.makedirs(target_dir, exist_ok=True)
    
    sucessos = 0
    for game_id in game_ids:
        url = BOX_URL.format(game_id=game_id)
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            boxscore_data = response.json()
            
            file_path = os.path.join(target_dir, f"boxscore_{game_id}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(boxscore_data, f, ensure_ascii=False, indent=4)
                
            sucessos += 1
            logging.info(f"Boxscore {game_id} salvo com sucesso.")
            
        except Exception as e:
            logging.error(f"Erro ao baixar boxscore {game_id}: {e}")
            
    logging.info(f"Extração concluída: {sucessos}/{len(game_ids)} boxscores salvos na camada Bronze.")

if __name__ == "__main__":
    # Para teste, vamos buscar os jogos de ontem 
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    extract_boxscores_to_datalake(ontem)