import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, expr

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuração de caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "schedule")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule")

def transform_schedule_bronze_to_silver():
    logging.info("Iniciando Spark Session (Simulando AWS Glue)...")
    
    spark = SparkSession.builder \
        .appName("NBA_Schedule_Silver") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    input_path = os.path.join(BRONZE_DIR, hoje_str, "schedule_league_raw.json")
    
    if not os.path.exists(input_path):
        logging.error(f"Arquivo Bronze não encontrado em: {input_path}")
        spark.stop()
        return

    logging.info(f"Lendo JSON bruto: {input_path}")
    
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    logging.info("Transformando e achatando a estrutura do JSON...")
    
    df_exploded = df_raw.select(explode("leagueSchedule.gameDates").alias("game_dates")) \
                        .select(explode("game_dates.games").alias("game"))

    # 3. Selecionar e Tipar as colunas de forma Segura (Data Quality)
    df_silver = df_exploded.select(
        col("game.gameId").alias("game_id"),
        to_timestamp(col("game.gameDateTimeUTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("game_datetime_utc"),
        col("game.gameStatus").cast("int").alias("game_status"),
        col("game.homeTeam.teamName").alias("home_team_name"),
        col("game.homeTeam.teamCity").alias("home_team_city"),
        col("game.awayTeam.teamName").alias("away_team_name"),
        col("game.awayTeam.teamCity").alias("away_team_city"),
        
        # AQUI ESTÁ A CORREÇÃO: Usamos get() via expr para retornar NULL se o array for vazio, em vez de quebrar o job
        expr("get(game.broadcasters.nationalTvBroadcasters, 0).broadcasterDisplay").alias("us_broadcaster")
    )

    df_silver = df_silver.filter(col("game_id").isNotNull())
    
    logging.info("Amostra dos dados transformados:")
    df_silver.show(5, truncate=False)
    
    output_path = os.path.join(SILVER_DIR, hoje_str)
    os.makedirs(output_path, exist_ok=True)
    
    logging.info(f"Salvando dados em Parquet na camada Silver: {output_path}")
    
    df_silver.write.mode("overwrite").parquet(output_path)
    
    logging.info("Processamento Silver concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    transform_schedule_bronze_to_silver()