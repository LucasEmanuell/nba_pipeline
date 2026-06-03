import json
import os
import logging
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, to_timestamp, expr,
    substring, when, regexp_extract, lit,
)

from src.spark_utils import get_spark_session

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
    # series_text e game_code_teams são descartados no select final — são andaimes, não dado.
    df_extracted = df_exploded.select(
        col("game.gameId").alias("game_id"),
        to_timestamp(col("game.gameDateTimeUTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("game_datetime_utc"),
        col("game.gameStatus").cast("int").alias("game_status"),
        col("game.homeTeam.teamName").alias("home_team_name"),
        col("game.homeTeam.teamCity").alias("home_team_city"),
        col("game.awayTeam.teamName").alias("away_team_name"),
        col("game.awayTeam.teamCity").alias("away_team_city"),
        expr("get(game.broadcasters.nationalTvBroadcasters, 0).broadcasterDisplay").alias("us_broadcaster"),
        col("game.seriesText").alias("series_text"),
        # gameCode formato "YYYYMMDD/AWYHME" — split pega a parte dos tricodes
        expr("split(game.gameCode, '/')[1]").alias("game_code_teams"),
    ).filter(col("game_id").isNotNull())

    # game_type derivado do prefixo do game_id — convenção da NBA:
    # 001 pré-temporada, 002 temporada regular, 004 playoffs, 005 play-in
    df_typed = df_extracted.withColumn(
        "game_type",
        when(substring(col("game_id"), 1, 3) == "002", "regular")
        .when(substring(col("game_id"), 1, 3) == "004", "playoff")
        .when(substring(col("game_id"), 1, 3) == "005", "play-in")
        .otherwise("preseason"),
    )

    # Series wins: só para jogos de playoff.
    # Formatos do seriesText: "NYK leads 3-1" | "Series tied 2-2" | "OKC wins 4-0"
    # Quando não há texto (Game 1, série não iniciada), retorna NULL — correto.
    series_leader = regexp_extract(col("series_text"), r"^(\w+)\s+(?:leads|wins)", 1)
    score_a = regexp_extract(col("series_text"), r"(\d+)-(\d+)", 1).cast("int")
    score_b = regexp_extract(col("series_text"), r"(\d+)-(\d+)", 2).cast("int")

    away_tricode = col("game_code_teams").substr(1, 3)
    home_tricode = col("game_code_teams").substr(4, 3)

    is_playoff = col("game_type") == "playoff"
    is_tied = series_leader == ""  # regexp_extract retorna "" quando não há match

    df_with_series = (
        df_typed
        .withColumn(
            "home_series_wins",
            when(~is_playoff, lit(None).cast("int"))
            .when(is_tied & score_a.isNotNull(), score_a)
            .when(series_leader == home_tricode, score_a)
            .when(series_leader == away_tricode, score_b)
            .otherwise(lit(None).cast("int")),
        )
        .withColumn(
            "away_series_wins",
            when(~is_playoff, lit(None).cast("int"))
            .when(is_tied & score_a.isNotNull(), score_a)
            .when(series_leader == away_tricode, score_a)
            .when(series_leader == home_tricode, score_b)
            .otherwise(lit(None).cast("int")),
        )
    )

    # Select final define o schema da Silver — colunas intermediárias ficam de fora
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
    df_silver.write.format("delta").mode("overwrite").save(output_path)
    df_silver.unpersist()

    spark.stop()


if __name__ == "__main__":
    transform_schedule_bronze_to_silver()
