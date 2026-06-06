import requests
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# A CDN da NBA bloqueia requests sem User-Agent. Imitamos um browser para evitar 403.
NBA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
}

# URL primária usa sufixo de versão; fallback é a canonical sem versão.
# A NBA às vezes atualiza o sufixo (_1, _2...) sem aviso — ter o fallback evita outage silencioso.
SCHEDULE_URLS = [
    "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_11.json",  # region=11 Brazil — tem intlTvBroadcasters
    "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json",
    "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json",
]

MIN_GAMES = 100  # calendário com menos jogos indica resposta truncada ou off-season

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")


def _download_schedule() -> dict:
    """Tenta cada URL em ordem. Levanta RuntimeError se todas falharem ou retornarem dado incompleto."""
    for url in SCHEDULE_URLS:
        source = url.split('/')[-1]
        try:
            response = requests.get(url, headers=NBA_HEADERS, timeout=15)
            response.raise_for_status()
            data = response.json()

            total_games = sum(len(d["games"]) for d in data["leagueSchedule"]["gameDates"])
            if total_games < MIN_GAMES:
                logger.warning(f"{source}: apenas {total_games} jogos (mínimo: {MIN_GAMES}) — tentando próxima fonte")
                continue

            logger.info(f"Calendário carregado: {source} ({total_games} jogos)")
            return data

        except (KeyError, ValueError) as e:
            logger.warning(f"{source}: estrutura inesperada — {e}")
        except requests.RequestException as e:
            logger.warning(f"{source}: falha na requisição — {e}")

    raise RuntimeError("Todas as fontes do calendário NBA falharam. Verificar logs acima.")


def extract_schedule_to_datalake():
    logger.info("Iniciando extração do calendário da NBA...")

    # _download_schedule levanta RuntimeError em caso de falha total.
    # Isso garante que o Airflow marque a task como FAILED e acione retries/alertas,
    # em vez de registrar sucesso com dado ausente.
    data = _download_schedule()

    extraction_date = datetime.now().strftime('%Y-%m-%d')
    target_dir = os.path.join(BRONZE_DIR, extraction_date)
    os.makedirs(target_dir, exist_ok=True)

    file_path = os.path.join(target_dir, "schedule_league_raw.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logger.info(f"Bronze: calendário salvo em {file_path}")


if __name__ == "__main__":
    extract_schedule_to_datalake()