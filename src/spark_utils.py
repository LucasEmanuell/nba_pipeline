from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str) -> SparkSession:
    """Retorna uma SparkSession configurada com suporte a Delta Lake.

    configure_spark_with_delta_pip injeta os JARs do Delta automaticamente
    quando delta-spark está instalado via pip — sem precisar baixar JARs manualmente.
    getOrCreate reutiliza a sessão se já existir na JVM corrente.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
