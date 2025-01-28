from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import os
import time

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("spotify-datalake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.13:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

start_time = time.time()

base_path = "/home/gustavocunha/CloudComputing4/deltalake"
os.makedirs(base_path, exist_ok=True)

# Carrega os dados iniciais
playlists_v1 = spark.read.json("/shared/sampled/playlists_v1.json")
tracks_v1 = spark.read.json("/shared/sampled/tracks_v1.json")
playlists_v2 = spark.read.json("/shared/sampled/playlists_v2.json")
tracks_v2 = spark.read.json("/shared/sampled/tracks_v2.json")
playlists_v3 = spark.read.json("/shared/sampled/playlists_v3.json")
tracks_v3 = spark.read.json("/shared/sampled/tracks_v3.json")

# Escreve os dados iniciais nas tabelas Silver
playlists_v1.write.format("delta").mode("overwrite").save(f"{base_path}/silver_playlists")
tracks_v1.write.format("delta").mode("overwrite").save(f"{base_path}/silver_tracks")
playlists_v1.join(tracks_v1, "pid").write.format("delta").mode("overwrite").save(f"{base_path}/gold")

# Função para tratar duplicidade e garantir uma correspondência única
def deduplicate_and_merge(df, target_path, merge_key, dedup_key):
    # Cria uma janela para aplicar a deduplicação
    window_spec = Window.partitionBy(merge_key).orderBy(col(dedup_key).desc())  # Use col() para referenciar a coluna
    deduplicated_df = df.withColumn("row_number", row_number().over(window_spec)) \
                        .filter(col("row_number") == 1) \
                        .drop("row_number")
    return deduplicated_df

# Atualiza a tabela Silver com playlists_v2
playlists_v2_dedup = deduplicate_and_merge(playlists_v2, f"{base_path}/silver_playlists", "pid", "modified")
playlists_v2_dedup.createOrReplaceTempView("playlists_v2_temp")
spark.sql(f"""
MERGE INTO delta.`{base_path}/silver_playlists` t
USING playlists_v2_temp s
ON t.pid = s.pid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Atualiza a tabela Silver com tracks_v2
tracks_v2_dedup = deduplicate_and_merge(tracks_v2, f"{base_path}/silver_tracks", "track_uri", "duration_ms")
tracks_v2_dedup.createOrReplaceTempView("tracks_v2_temp")
spark.sql(f"""
MERGE INTO delta.`{base_path}/silver_tracks` t
USING tracks_v2_temp s
ON t.track_uri = s.track_uri
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Atualiza playlists específicas na tabela Silver
silver_playlists = spark.read.format("delta").load(f"{base_path}/silver_playlists")
updated_playlists = silver_playlists.filter(col("pid") == 11992).withColumn("modified", col("modified") + 1)
updated_playlists.createOrReplaceTempView("updated_playlists")
spark.sql(f"""
MERGE INTO delta.`{base_path}/silver_playlists` t
USING updated_playlists s
ON t.pid = s.pid
WHEN MATCHED THEN UPDATE SET *
""")

# Atualiza a tabela Silver com playlists_v3
playlists_v3_dedup = deduplicate_and_merge(playlists_v3, f"{base_path}/silver_playlists", "pid", "modified")
playlists_v3_dedup.createOrReplaceTempView("playlists_v3_temp")
spark.sql(f"""
MERGE INTO delta.`{base_path}/silver_playlists` t
USING playlists_v3_temp s
ON t.pid = s.pid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Atualiza a tabela Silver com tracks_v3
tracks_v3_dedup = deduplicate_and_merge(tracks_v3, f"{base_path}/silver_tracks", "track_uri", "duration_ms")
tracks_v3_dedup.createOrReplaceTempView("tracks_v3_temp")
spark.sql(f"""
MERGE INTO delta.`{base_path}/silver_tracks` t
USING tracks_v3_temp s
ON t.track_uri = s.track_uri
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Atualiza a tabela Gold
silver_playlists = spark.read.format("delta").load(f"{base_path}/silver_playlists")
silver_tracks = spark.read.format("delta").load(f"{base_path}/silver_tracks")
silver_playlists.join(silver_tracks, "pid").write.format("delta").mode("overwrite").save(f"{base_path}/gold")

# Calcula métricas de tamanho e tempo
end_time = time.time()
elapsed_time = end_time - start_time

parquet_size = sum(os.path.getsize(os.path.join(dp, f)) for dp, dn, filenames in os.walk('/shared/sampled/') for f in filenames if f.endswith('.json'))
delta_size = sum(os.path.getsize(os.path.join(dp, f)) for dp, dn, filenames in os.walk(base_path) for f in filenames)

print(f"Elapsed time: {elapsed_time:.2f} seconds")
print(f"Parquet size: {parquet_size / (1024 * 1024):.2f} MB")
print(f"Delta size: {delta_size / (1024 * 1024):.2f} MB")

spark.stop()
