FROM apache/airflow:2.9.0

USER root

# Atualiza o Linux e instala o Java
RUN apt-get update \
  && apt-get install -y default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Volta para o usuário seguro do Airflow
USER airflow

# Instala todas as bibliotecas Python que o seu projeto precisa
RUN pip install --no-cache-dir pyspark delta-spark deltalake beautifulsoup4 requests pandas sqlalchemy psycopg2-binary pyarrow