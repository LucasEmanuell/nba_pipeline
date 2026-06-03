import os
import logging
from datetime import datetime
from pyspark.sql.functions import col, to_date, from_utc_timestamp, expr

from src.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NBA_SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule")
JUMPER_SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "jumper_brasil")
ENRICHED_SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule_enriched")


def enrich_schedule_with_brazil_tv():
    spark = get_spark_session("NBA_Enriched_Schedule")
    spark.sparkContext.setLogLevel("WARN")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    nba_path = os.path.join(NBA_SILVER_DIR, hoje_str)
    jumper_path = os.path.join(JUMPER_SILVER_DIR, hoje_str)

    if not os.path.exists(nba_path):
        logger.error(f"Silver Schedule não encontrado: {nba_path}")
        spark.stop()
        raise FileNotFoundError(nba_path)

    if not os.path.exists(jumper_path):
        logger.error(f"Silver Jumper não encontrado: {jumper_path}")
        spark.stop()
        raise FileNotFoundError(jumper_path)

    logger.info("Lendo Silver Schedule (Delta)...")
    df_nba = spark.read.format("delta").load(nba_path)

    # Converte UTC para horário de Brasília para fazer o join por data local.
    # America/Sao_Paulo respeita DST — diferente de subtrair timedelta(hours=-3) fixo.
    df_nba = (
        df_nba
        .withColumn("game_datetime_br", from_utc_timestamp(col("game_datetime_utc"), "America/Sao_Paulo"))
        .withColumn("game_date_br", to_date(col("game_datetime_br")))
    )

    logger.info("Lendo Silver Jumper (Delta)...")
    # Silver Jumper já tem game_date tipado como Date e nomes canônicos.
    # Não há transformação aqui — a Silver cumpriu seu contrato na camada anterior.
    df_jumper = spark.read.format("delta").load(jumper_path)

    logger.info("Cruzando calendário NBA com transmissões do Brasil...")
    join_cond = (
        (col("game_date_br") == col("game_date")) &
        expr("lower(home_team_raw) LIKE concat('%', lower(home_team_name), '%')") &
        expr("lower(away_team_raw) LIKE concat('%', lower(away_team_name), '%')")
    )

    df_enriched = df_nba.join(df_jumper, join_cond, "left")

    # O select define o schema da enriched — inclui os novos campos de playoff
    df_final = df_enriched.select(
        col("game_id"),
        col("game_datetime_utc"),
        col("game_status"),
        col("game_type"),
        col("home_team_city"),
        col("home_team_name"),
        col("away_team_city"),
        col("away_team_name"),
        col("us_broadcaster"),
        col("home_series_wins"),
        col("away_series_wins"),
        col("brazil_broadcaster"),
    )

    com_br = df_final.filter(col("brazil_broadcaster").isNotNull()).count()
    total = df_final.count()
    logger.info(f"Jogos com transmissão no Brasil: {com_br}/{total}")

    output_path = os.path.join(ENRICHED_SILVER_DIR, hoje_str)
    logger.info(f"Salvando Silver Enriched em Delta: {output_path}")
    df_final.write.format("delta").mode("overwrite").save(output_path)

    spark.stop()


if __name__ == "__main__":
    enrich_schedule_with_brazil_tv()
