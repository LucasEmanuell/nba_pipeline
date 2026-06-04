import os
import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import col, explode, lit

from src.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "player_stats")


def _extract_side(df_raw, side: str, team_col: str):
    """Extrai e achata os jogadores de um lado (home/away) para um schema fixo.

    O struct de cada jogador pode ter campos extras (notPlayingDescription, notPlayingReason)
    dependendo se jogou ou nao. Selecionar campos especificos antes do union garante
    que home e away tenham o mesmo schema, evitando INCOMPATIBLE_COLUMN_TYPE.
    """
    return (
        df_raw
        .select(
            col("game.gameId").alias("game_id"),
            col(f"game.{team_col}.teamId").alias("team_id"),
            col(f"game.{team_col}.teamCity").alias("team_city"),
            col(f"game.{team_col}.teamName").alias("team_name"),
            explode(col(f"game.{team_col}.players")).alias("player"),
        )
        .select(
            col("game_id"),
            col("team_id"),
            col("team_city"),
            col("team_name"),
            lit(side).alias("side"),
            col("player.personId").alias("player_id"),
            col("player.name").alias("player_name"),
            col("player.position").alias("position"),
            col("player.starter").cast("int").alias("starter"),
            col("player.played").alias("played"),
            col("player.statistics.minutes").alias("minutes"),
            col("player.statistics.points").cast("int").alias("pts"),
            col("player.statistics.reboundsTotal").cast("int").alias("reb"),
            col("player.statistics.assists").cast("int").alias("ast"),
            col("player.statistics.steals").cast("int").alias("stl"),
            col("player.statistics.blocks").cast("int").alias("blk"),
            col("player.statistics.turnovers").cast("int").alias("tov"),
            col("player.statistics.plusMinusPoints").cast("int").alias("plus_minus"),
            col("player.statistics.fieldGoalsMade").cast("int").alias("fgm"),
            col("player.statistics.fieldGoalsAttempted").cast("int").alias("fga"),
            col("player.statistics.fieldGoalsPercentage").cast("double").alias("fg_pct"),
            col("player.statistics.threePointersMade").cast("int").alias("tpm"),
            col("player.statistics.threePointersAttempted").cast("int").alias("tpa"),
            col("player.statistics.threePointersPercentage").cast("double").alias("tp_pct"),
            col("player.statistics.freeThrowsMade").cast("int").alias("ftm"),
            col("player.statistics.freeThrowsAttempted").cast("int").alias("fta"),
            col("player.statistics.freeThrowsPercentage").cast("double").alias("ft_pct"),
            col("player.statistics.reboundsOffensive").cast("int").alias("reb_off"),
            col("player.statistics.reboundsDefensive").cast("int").alias("reb_def"),
        )
    )


def transform_player_stats_bronze_to_silver(target_date: str, stop_spark: bool = True):
    spark = get_spark_session("NBA_PlayerStats_Silver")
    spark.sparkContext.setLogLevel("WARN")

    bronze_dir = os.path.join(BRONZE_DIR, target_date)
    if not os.path.exists(bronze_dir):
        logger.warning(f"Nenhum boxscore na Bronze para {target_date}, pulando player stats.")
        if stop_spark:
            spark.stop()
        return

    input_path = os.path.join(bronze_dir, "*.json")
    logger.info(f"Extraindo player stats de: {input_path}")

    df_raw = spark.read.option("multiline", "true").json(input_path)

    # schema achatado antes do union, pois jogadores DNP tem campos extras no struct
    df_home = _extract_side(df_raw, "home", "homeTeam")
    df_away = _extract_side(df_raw, "away", "awayTeam")

    df_silver = (
        df_home.union(df_away)
        .filter(col("played") == "1")
        .drop("played")
    )

    df_silver.cache()
    count = df_silver.count()

    if count == 0:
        logger.warning(f"Nenhum player stat extraido para {target_date}, todos DNP?")
        df_silver.unpersist()
        if stop_spark:
            spark.stop()
        return

    logger.info(f"Quality OK: {count} linhas de player stats para {target_date}")

    output_path = os.path.join(SILVER_DIR, target_date)
    logger.info(f"Salvando Silver Player Stats em Delta: {output_path}")
    df_silver.write.format("delta").mode("overwrite").save(output_path)
    df_silver.unpersist()

    if stop_spark:
        spark.stop()


if __name__ == "__main__":
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_player_stats_bronze_to_silver(ontem)
