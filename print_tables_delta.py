from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("delta-lake-inspect") \
    .config("spark.jars.packages", "io.delta:delta-core_2.13:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

silver_path = "deltalake/silver"
gold_path = "deltalake/gold"

def show_first_10_rows(table_path):
    try:
        df = spark.read.format("delta").load(table_path)
        df.show(10, truncate=False)
    except Exception as e:
        print(f"Erro ao ler a tabela em {table_path}: {e}")


tables = [
    f"{silver_path}/song_info",
    f"{silver_path}/album_info",
    f"{silver_path}/artist_info",
    f"{silver_path}/playlist_info",
    f"{silver_path}/playlist_tracks",
    f"{gold_path}/gold_playlist_info",
    f"{gold_path}/gold_playlist_tracks"
]

for table in tables:
    print(f"Primeiras 10 linhas de {table}:")
    show_first_10_rows(table)
    print("-" * 50)

spark.stop()
