import json
import logging
import os
import time
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOX_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

NBA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
}

DELAY_SECONDS = 1.0  # respeita a CDN da NBA sem tomar ban

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_SCHEDULE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")
BRONZE_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")


def _find_latest_schedule() -> str:
    """Retorna o path do schedule bronze mais recente disponivel."""
    dates = sorted(os.listdir(BRONZE_SCHEDULE_DIR), reverse=True)
    if not dates:
        raise FileNotFoundError("Nenhum schedule bronze encontrado. Rode get_nba.py primeiro.")
    return os.path.join(BRONZE_SCHEDULE_DIR, dates[0], "schedule_league_raw.json")


def _get_completed_games_by_date(schedule_path: str) -> dict:
    """Le o schedule bronze e retorna {data: [game_ids]} apenas para jogos finalizados."""
    with open(schedule_path, encoding="utf-8") as f:
        data = json.load(f)

    games_by_date = {}
    for day in data["leagueSchedule"]["gameDates"]:
        raw_date = day.get("gameDate", "")
        try:
            dt = datetime.strptime(raw_date.split(" ")[0], "%m/%d/%Y")
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            continue

        for game in day["games"]:
            if game.get("gameStatus") == 3:
                game_id = game.get("gameId")
                if game_id:
                    games_by_date.setdefault(date_str, []).append(game_id)

    return games_by_date


def run_backfill():
    schedule_path = _find_latest_schedule()
    logger.info(f"Lendo schedule de: {schedule_path}")

    games_by_date = _get_completed_games_by_date(schedule_path)
    total_games = sum(len(ids) for ids in games_by_date.values())
    logger.info(f"Backfill: {total_games} jogos finalizados em {len(games_by_date)} datas")

    downloaded = skipped = errors = 0

    for date_str, game_ids in sorted(games_by_date.items()):
        target_dir = os.path.join(BRONZE_BOXSCORE_DIR, date_str)
        os.makedirs(target_dir, exist_ok=True)

        for game_id in game_ids:
            file_path = os.path.join(target_dir, f"boxscore_{game_id}.json")

            # idempotente: pula se ja foi baixado
            if os.path.exists(file_path):
                skipped += 1
                continue

            try:
                response = requests.get(BOX_URL.format(game_id=game_id), headers=NBA_HEADERS, timeout=15)
                response.raise_for_status()

                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(response.json(), f, ensure_ascii=False, indent=4)

                downloaded += 1
                time.sleep(DELAY_SECONDS)

            except requests.RequestException as e:
                logger.error(f"Erro ao baixar boxscore {game_id}: {e}")
                errors += 1

        logger.info(f"{date_str}: {len(game_ids)} jogos | baixados: {downloaded}, pulados: {skipped}, erros: {errors}")

    logger.info(f"Extracao concluida: {downloaded} baixados, {skipped} ja existiam, {errors} erros")


if __name__ == "__main__":
    run_backfill()
