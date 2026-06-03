import os
import logging
from datetime import datetime
from pyspark.sql.functions import col, to_date, trim

from src.spark_utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "jumper_brasil")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "jumper_brasil")


def transform_jumper_bronze_to_silver():
    spark = get_spark_session("NBA_Jumper_Silver")
    spark.sparkContext.setLogLevel("WARN")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    input_path = os.path.join(BRONZE_DIR, hoje_str, "canais_jumper_raw.csv")

    if not os.path.exists(input_path):
        logger.error(f"Bronze Jumper não encontrado: {input_path}")
        spark.stop()
        raise FileNotFoundError(input_path)

    logger.info(f"Lendo CSV Bronze: {input_path}")
    df_raw = spark.read.option("header", "true").csv(input_path)

    # Responsabilidade desta camada: tipar, limpar espaços, renomear para nomes canônicos.
    # Sem lógica de join ou enriquecimento — isso é trabalho da enriched.
    df_silver = df_raw.select(
        to_date(col("data_br"), "dd/MM/yy").alias("game_date"),
        trim(col("visitante")).alias("away_team_raw"),
        trim(col("mandante")).alias("home_team_raw"),
        col("horario_br").alias("kickoff_time_br"),
        trim(col("canal_br")).alias("brazil_broadcaster"),
    ).filter(col("game_date").isNotNull())

    record_count = df_silver.count()
    output_path = os.path.join(SILVER_DIR, hoje_str)

    logger.info(f"Salvando Silver Jumper em Delta: {output_path} ({record_count} registros)")
    df_silver.write.format("delta").mode("overwrite").save(output_path)

    spark.stop()


if __name__ == "__main__":
    transform_jumper_bronze_to_silver()
