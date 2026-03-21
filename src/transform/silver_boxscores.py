import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "bronze", "boxscores")
SILVER_BOXSCORE_DIR = os.path.join(BASE_DIR, "data", "silver", "boxscores")

def transform_boxscores_bronze_to_silver(target_date: str):
    logging.info(f"Iniciando Spark Session para processar Boxscores do dia {target_date}...")
    
    spark = SparkSession.builder \
        .appName("NBA_Boxscores_Silver") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    input_path = os.path.join(BRONZE_BOXSCORE_DIR, target_date, "*.json") # Lê todos os JSONs da pasta
    
    # Verifica se o diretório existe
    if not os.path.exists(os.path.dirname(input_path.replace('*.json', ''))):
        logging.warning(f"Nenhum boxscore encontrado na Bronze para a data {target_date}.")
        spark.stop()
        return

    logging.info(f"Lendo JSONs brutos: {input_path}")
    
    # Lendo os múltiplos JSONs de uma vez (o Spark é ótimo nisso)
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    logging.info("Transformando e extraindo pontuações...")
    
    # Selecionando e tipando as colunas
    df_silver = df_raw.select(
        col("game.gameId").alias("game_id"),
        col("game.gameStatus").cast("int").alias("game_status"),
        col("game.awayTeam.teamCity").alias("away_team_city"),
        col("game.awayTeam.teamName").alias("away_team_name"),
        col("game.awayTeam.score").cast("int").alias("away_score"),
        col("game.homeTeam.teamCity").alias("home_team_city"),
        col("game.homeTeam.teamName").alias("home_team_name"),
        col("game.homeTeam.score").cast("int").alias("home_score")
    )

    # Regra de Negócio: Quem ganhou? e o jogo já acabou?
    df_silver = df_silver.withColumn(
        "is_final", 
        col("game_status") == 3
    ).withColumn(
        "winner",
        when(col("away_score") > col("home_score"), "away")
        .when(col("home_score") > col("away_score"), "home")
        .otherwise("data_anomaly") # Captura erros da API da NBA
    )

    logging.info("Amostra dos Boxscores transformados:")
    df_silver.show(truncate=False)
    
    output_path = os.path.join(SILVER_BOXSCORE_DIR, target_date)
    os.makedirs(output_path, exist_ok=True)
    
    logging.info(f"Salvando Boxscores em Parquet na camada Silver: {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)
    
    logging.info("Processamento de Boxscores concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    # Vamos processar os jogos de ontem (já que os boxscores que baixamos são de ontem)
    ontem = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    transform_boxscores_bronze_to_silver(ontem)