import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_utc_timestamp, expr

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NBA_SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule")
JUMPER_BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze", "jumper_brasil")
ENRICHED_SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "schedule_enriched")

def enrich_schedule_with_brazil_tv():
    logging.info("Iniciando Spark Session (Data Enrichment)...")
    
    spark = SparkSession.builder \
        .appName("NBA_Enriched_Schedule") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    hoje_str = datetime.now().strftime('%Y-%m-%d')
    nba_path = os.path.join(NBA_SILVER_DIR, hoje_str)
    jumper_path = os.path.join(JUMPER_BRONZE_DIR, hoje_str, "canais_jumper_raw.csv")

    if not os.path.exists(nba_path) or not os.path.exists(jumper_path):
        logging.error("Arquivos de origem não encontrados. Execute as extrações primeiro.")
        spark.stop()
        return

    logging.info("Lendo base oficial da NBA (Parquet)...")
    df_nba = spark.read.parquet(nba_path)
    
    # 1. Ajuste de Timezone: Cria coluna de Data no fuso de SP apenas para o Join
    df_nba = df_nba.withColumn("game_datetime_br", from_utc_timestamp(col("game_datetime_utc"), "America/Sao_Paulo"))
    df_nba = df_nba.withColumn("data_jogo_br", to_date(col("game_datetime_br")))

    logging.info("Lendo base do Jumper Brasil (CSV Bruto)...")
    df_jumper = spark.read.option("header", "true").csv(jumper_path)
    
    # 2. Ajuste de Data Jumper: De 'dd/MM/yy' para Data real
    df_jumper = df_jumper.withColumn("data_br_formatada", to_date(col("data_br"), "dd/MM/yy"))
    
    # 3. O JOIN Mágico: Data exata no BR + 'LIKE' no nome do time
    logging.info("Cruzando os dados (JOIN)...")
    join_cond = (
        (col("data_jogo_br") == col("data_br_formatada")) &
        # O mandante do Jumper tem que conter o 'team_name' da NBA (ex: 'Los Angeles Lakers' contém 'Lakers')
        expr("lower(mandante) LIKE concat('%', lower(home_team_name), '%')") &
        expr("lower(visitante) LIKE concat('%', lower(away_team_name), '%')")
    )

    df_enriched = df_nba.join(df_jumper, join_cond, "left")

    # 4. Seleciona apenas o que importa (mantendo o calendário oficial e adicionando o Brasil)
    df_final = df_enriched.select(
        col("game_id"),
        col("game_datetime_utc"),
        col("game_status"),
        col("home_team_city"),
        col("home_team_name"),
        col("away_team_city"),
        col("away_team_name"),
        col("us_broadcaster"),
        col("canal_br").alias("brazil_broadcaster")
    )

    logging.info("Amostra dos jogos que POSSUEM transmissão no Brasil:")
    # Filtra só para mostrar no terminal que o cruzamento deu certo
    df_final.filter(col("brazil_broadcaster").isNotNull()).show(5, truncate=False)

    # 5. Salva a nova tabela enriquecida
    output_path = os.path.join(ENRICHED_SILVER_DIR, hoje_str)
    os.makedirs(output_path, exist_ok=True)
    
    logging.info(f"Salvando calendário enriquecido em Parquet: {output_path}")
    df_final.write.mode("overwrite").parquet(output_path)
    
    logging.info("Enriquecimento concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    enrich_schedule_with_brazil_tv()