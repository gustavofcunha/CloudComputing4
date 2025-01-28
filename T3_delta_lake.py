from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, count, countDistinct, sum
import os
from time import time

playlists_v1_path = '/shared/sampled/playlists_v1.json'
tracks_v1_path = '/shared/sampled/tracks_v1.json'
playlists_v2_path = '/shared/sampled/playlists_v2.json'
tracks_v2_path = '/shared/sampled/tracks_v2.json'
playlists_v3_path = '/shared/sampled/playlists_v3.json'
tracks_v3_path = '/shared/sampled/tracks_v3.json'

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

start_time = time()

# Read all versions
playlists_v1_df = spark.read.json(playlists_v1_path)
tracks_v1_df = spark.read.json(tracks_v1_path)
playlists_v2_df = spark.read.json(playlists_v2_path)
tracks_v2_df = spark.read.json(tracks_v2_path)
playlists_v3_df = spark.read.json(playlists_v3_path)
tracks_v3_df = spark.read.json(tracks_v3_path)

# Silver Zone tables
song_info_df = tracks_v2_df.select(
    col("track_name").alias("song_name"),
    col("track_uri"),
    col("duration_ms").cast("long").alias("duration"),  
    col("album_uri"),
    col("artist_uri")
)

album_info_df = tracks_v2_df.select(
    col("album_name"),
    col("album_uri"),
    col("track_uri"),
    col("artist_uri")
)

artist_info_df = tracks_v2_df.select(
    col("artist_name").alias("artist_name"),
    col("artist_uri")
)

playlist_info_df = playlists_v2_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative").cast("boolean")
)

playlist_tracks_df = tracks_v2_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("artist_name"),
    col("album_uri"),
    col("album_name"),
    col("duration_ms").cast("long").alias("duration")  # Ensure duration is long
)

# Gold Zone tables
gold_playlist_info_df = playlist_info_df.join(
    playlist_tracks_df.groupBy("playlist_id")
    .agg(
        count("track_uri").alias("number_of_tracks"),
        countDistinct("artist_uri").alias("number_of_artists"),
        countDistinct("album_uri").alias("number_of_albums"),
        sum("duration").alias("total_duration")  # Sum duration as long
    ),
    "playlist_id"
)

gold_playlist_tracks_df = playlist_tracks_df.join(
    song_info_df, "track_uri", "inner"
).select(
    "playlist_id",
    "position",
    "song_name",
    "album_name",
    "artist_name"
)

delta_path = "deltalake"
silver_path = f"{delta_path}/silver"
gold_path = f"{delta_path}/gold"

song_info_df.write.format("delta").mode("overwrite").save(f"{silver_path}/song_info")
album_info_df.write.format("delta").mode("overwrite").save(f"{silver_path}/album_info")
artist_info_df.write.format("delta").mode("overwrite").save(f"{silver_path}/artist_info")
playlist_info_df.write.format("delta").mode("overwrite").save(f"{silver_path}/playlist_info")
playlist_tracks_df.write.format("delta").mode("overwrite").save(f"{silver_path}/playlist_tracks")

gold_playlist_info_df.write.format("delta").mode("overwrite").save(f"{gold_path}/gold_playlist_info")
gold_playlist_tracks_df.write.format("delta").mode("overwrite").save(f"{gold_path}/gold_playlist_tracks")

playlist_info_v1_df = spark.read.format("delta").load(f"{silver_path}/playlist_info")
playlist_tracks_v1_df = spark.read.format("delta").load(f"{silver_path}/playlist_tracks")

playlist_info_v2_df = playlists_v2_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative").cast("boolean")
)

playlist_info_merged_df = playlist_info_v1_df.alias("v1").join(
    playlist_info_v2_df.alias("v2"),
    "playlist_id",
    "outer"
).select(
    coalesce("v2.playlist_name", "v1.playlist_name").alias("playlist_name"),
    coalesce("v2.playlist_id", "v1.playlist_id").alias("playlist_id"),
    coalesce("v2.description", "v1.description").alias("description"),
    coalesce("v2.collaborative", "v1.collaborative").alias("collaborative")
)

playlist_info_updated_df = playlist_info_merged_df.withColumn(
    "playlist_name",
    when(col("playlist_id") == 11992, "Updated Playlist Name").otherwise(col("playlist_name"))
)

playlist_info_v3_df = playlists_v3_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative").cast("boolean")
)

playlist_info_final_df = playlist_info_updated_df.alias("v2").join(
    playlist_info_v3_df.alias("v3"),
    "playlist_id",
    "outer"
).select(
    coalesce("v3.playlist_name", "v2.playlist_name").alias("playlist_name"),
    coalesce("v3.playlist_id", "v2.playlist_id").alias("playlist_id"),
    coalesce("v3.description", "v2.description").alias("description"),
    coalesce("v3.collaborative", "v2.collaborative").alias("collaborative")
)

playlist_info_final_df.write.format("delta").mode("overwrite").save(f"{silver_path}/playlist_info")

total_time = time() - start_time

storage_size = 0
for root, dirs, files in os.walk(delta_path):
    for file in files:
        storage_size += os.path.getsize(os.path.join(root, file))

print(f"Total time for operations: {total_time:.2f} seconds")
print(f"Total space used by Delta Lake: {storage_size / (1024 * 1024):.2f} MB")

spark.stop()