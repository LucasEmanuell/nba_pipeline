import os
import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import col, when

from src.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "boxscores")


def transform_boxscores_bronze_to_silver(target_date: str):
    spark = get_spark_session("NBA_Boxscores_Silver")
    spark.sparkContext.setLogLevel("WARN")

    bronze_dir = os.path.join(BRONZE_DIR, target_date)
    if not os.path.exists(bronze_dir):
        logger.warning(f"Nenhum boxscore na Bronze para {target_date} — data sem jogos ou extração pendente.")
        spark.stop()
        return

    input_path = os.path.join(bronze_dir, "*.json")
    logger.info(f"Lendo boxscores: {input_path}")

    df_raw = spark.read.option("multiline", "true").json(input_path)

    df_silver = df_raw.select(
        col("game.gameId").alias("game_id"),
        col("game.gameStatus").cast("int").alias("game_status"),
        col("game.awayTeam.teamCity").alias("away_team_city"),
        col("game.awayTeam.teamName").alias("away_team_name"),
        col("game.awayTeam.score").cast("int").alias("away_score"),
        col("game.homeTeam.teamCity").alias("home_team_city"),
        col("game.homeTeam.teamName").alias("home_team_name"),
        col("game.homeTeam.score").cast("int").alias("home_score"),
    ).withColumn(
        "is_final",
        col("game_status") == 3,
    ).withColumn(
        "winner",
        when(col("away_score") > col("home_score"), "away")
        .when(col("home_score") > col("away_score"), "home")
        .otherwise("data_anomaly"),
    )

    output_path = os.path.join(SILVER_DIR, target_date)
    logger.info(f"Salvando Silver Boxscores em Delta: {output_path}")
    df_silver.write.format("delta").mode("overwrite").save(output_path)

    logger.info(f"Silver Boxscores: {df_silver.count()} jogos processados para {target_date}")
    spark.stop()


if __name__ == "__main__":
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_boxscores_bronze_to_silver(ontem)
