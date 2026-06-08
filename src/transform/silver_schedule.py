import json
import os
import logging
from datetime import datetime
from pyspark.sql.functions import col, explode, to_timestamp, expr, get_json_object, udf
from pyspark.sql.types import StringType, IntegerType

from src.spark_utils import get_spark_session
from src.transform.derivations import derive_game_type, parse_series_wins

_game_type_udf = udf(derive_game_type, StringType())


def _home_wins(series_text, home_tricode, away_tricode, game_type):
    if game_type != "playoff":
        return None
    wins, _ = parse_series_wins(series_text, home_tricode, away_tricode)
    return wins


def _away_wins(series_text, home_tricode, away_tricode, game_type):
    if game_type != "playoff":
        return None
    _, wins = parse_series_wins(series_text, home_tricode, away_tricode)
    return wins


_home_wins_udf = udf(_home_wins, IntegerType())
_away_wins_udf = udf(_away_wins, IntegerType())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule")


def transform_schedule_bronze_to_silver():
    spark = get_spark_session("NBA_Schedule_Silver")
    spark.sparkContext.setLogLevel("WARN")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    input_path = os.path.join(BRONZE_DIR, hoje_str, "schedule_league_raw.json")

    if not os.path.exists(input_path):
        logger.error(f"Bronze não encontrado: {input_path}")
        spark.stop()
        raise FileNotFoundError(input_path)

    logger.info(f"Lendo JSON bruto: {input_path}")
    df_raw = spark.read.option("multiline", "true").json(input_path)

    df_exploded = (
        df_raw
        .select(explode("leagueSchedule.gameDates").alias("game_dates"))
        .select(explode("game_dates.games").alias("game"))
    )

    # Extração inicial: colunas de negócio + campos intermediários para derivações.
    # series_text e game_code_teams são descartados no select final, são andaimes pra derivações.
    df_extracted = df_exploded.select(
        col("game.gameId").alias("game_id"),
        to_timestamp(col("game.gameDateTimeUTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("game_datetime_utc"),
        col("game.gameStatus").cast("int").alias("game_status"),
        col("game.homeTeam.teamName").alias("home_team_name"),
        col("game.homeTeam.teamCity").alias("home_team_city"),
        col("game.awayTeam.teamName").alias("away_team_name"),
        col("game.awayTeam.teamCity").alias("away_team_city"),
        # cast para string antes do get_json_object é necessário pois o JSON regional _11
        # pode inferir o campo de broadcaster como STRING, ArrayType(StringType) ou
        # ArrayType(StructType) dependendo dos dados, cast normaliza tudo.
        get_json_object(col("game.broadcasters.nationalTvBroadcasters").cast("string"), "$[0].broadcasterDisplay").alias("us_broadcaster"),
        get_json_object(col("game.broadcasters.intlTvBroadcasters").cast("string"), "$[0].broadcasterDisplay").alias("brazil_broadcaster"),
        col("game.seriesText").alias("series_text"),
        # gameCode formato "YYYYMMDD/AWYHME", split pega a parte dos tricodes
        expr("split(game.gameCode, '/')[1]").alias("game_code_teams"),
    ).filter(col("game_id").isNotNull())

    df_typed = df_extracted.withColumn("game_type", _game_type_udf(col("game_id")))

    home_tricode = col("game_code_teams").substr(4, 3)
    away_tricode = col("game_code_teams").substr(1, 3)

    df_with_series = (
        df_typed
        .withColumn("home_series_wins", _home_wins_udf(col("series_text"), home_tricode, away_tricode, col("game_type")))
        .withColumn("away_series_wins", _away_wins_udf(col("series_text"), home_tricode, away_tricode, col("game_type")))
    )

    # select final define o schema da Silver, colunas intermediárias ficam de fora
    df_silver = df_with_series.select(
        "game_id",
        "game_datetime_utc",
        "game_status",
        "game_type",
        "home_team_city",
        "home_team_name",
        "away_team_city",
        "away_team_name",
        "us_broadcaster",
        "brazil_broadcaster",
        "home_series_wins",
        "away_series_wins",
    )

    # conta os jogos do Bronze com Python puro, mais rapido que rodar o Spark de novo
    with open(input_path) as f:
        raw = json.load(f)
    bronze_count = sum(len(d["games"]) for d in raw["leagueSchedule"]["gameDates"])

    # cache evita que o Spark avalie o plano duas vezes, uma no count e outra no write
    df_silver.cache()
    silver_count = df_silver.count()

    if silver_count == 0:
        raise ValueError("Silver Schedule vazia apos transformacao")

    loss_pct = (bronze_count - silver_count) / bronze_count
    if loss_pct > 0.20:
        raise ValueError(
            f"Silver perdeu {loss_pct:.0%} dos registros do Bronze "
            f"({silver_count}/{bronze_count}) - acima do limite de 20%"
        )

    unique_ids = df_silver.select("game_id").distinct().count()
    if unique_ids != silver_count:
        raise ValueError(
            f"game_id nao e unico: {silver_count} registros, {unique_ids} ids distintos"
        )

    logger.info(f"Quality OK: {silver_count} jogos, {unique_ids} ids unicos ({loss_pct:.1%} de perda vs Bronze)")

    output_path = os.path.join(SILVER_DIR, hoje_str)
    logger.info(f"Salvando Silver Schedule em Delta: {output_path}")
    df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    df_silver.unpersist()

    spark.stop()


if __name__ == "__main__":
    transform_schedule_bronze_to_silver()
