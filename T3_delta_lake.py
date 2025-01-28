from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from time import time
import os

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

playlists_v1_df = spark.read.json(playlists_v1_path)
tracks_v1_df = spark.read.json(tracks_v1_path)
playlists_v2_df = spark.read.json(playlists_v2_path)
tracks_v2_df = spark.read.json(tracks_v2_path)
playlists_v3_df = spark.read.json(playlists_v3_path)
tracks_v3_df = spark.read.json(tracks_v3_path)

new_playlists_v2_df = playlists_v2_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative").cast("boolean")
)

new_tracks_v2_df = tracks_v2_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("album_uri"),
    col("duration_ms").alias("duration")
)

new_playlists_v3_df = playlists_v3_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative").cast("boolean")
)

new_tracks_v3_df = tracks_v3_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("album_uri"),
    col("duration_ms").alias("duration")
)

delta_path = "datalake/delta"
playlist_info_df = spark.createDataFrame([], new_playlists_v2_df.schema)
playlist_tracks_df = spark.createDataFrame([], new_tracks_v2_df.schema)

playlist_info_df = playlist_info_df.unionByName(new_playlists_v2_df, allowMissingColumns=True).dropDuplicates(["playlist_id"])

update_playlist_df = spark.createDataFrame([(11992, "GYM WORKOUT", True)], ["playlist_id", "playlist_name", "collaborative"])
update_playlist_df = update_playlist_df.withColumn("collaborative", col("collaborative").cast("boolean"))

playlist_info_df = playlist_info_df.alias("playlist_info").join(update_playlist_df.alias("update_playlist"), "playlist_id", "left").select(
    "playlist_info.playlist_id",
    when(col("update_playlist.playlist_name").isNotNull(), col("update_playlist.playlist_name")).otherwise(col("playlist_info.playlist_name")).alias("playlist_name"),
    when(col("update_playlist.collaborative").isNotNull(), col("update_playlist.collaborative")).otherwise(col("playlist_info.collaborative")).alias("collaborative"),
    "playlist_info.description"
)

playlist_info_df = playlist_info_df.unionByName(new_playlists_v3_df, allowMissingColumns=True).dropDuplicates(["playlist_id"])

playlist_tracks_df = playlist_tracks_df.alias("old").join(new_tracks_v3_df.alias("new"), 
    (col("old.playlist_id") == col("new.playlist_id")) & 
    (col("old.position") == col("new.position")), 
    "outer"
).select(
    when(col("new.playlist_id").isNotNull(), col("new.playlist_id")).otherwise(col("old.playlist_id")).alias("playlist_id"),
    when(col("new.position").isNotNull(), col("new.position")).otherwise(col("old.position")).alias("position"),
    when(col("new.track_uri").isNotNull(), col("new.track_uri")).otherwise(col("old.track_uri")).alias("track_uri"),
    when(col("new.artist_uri").isNotNull(), col("new.artist_uri")).otherwise(col("old.artist_uri")).alias("artist_uri"),
    when(col("new.album_uri").isNotNull(), col("new.album_uri")).otherwise(col("old.album_uri")).alias("album_uri"),
    when(col("new.duration").isNotNull(), col("new.duration")).otherwise(col("old.duration")).alias("duration")
).dropDuplicates(["playlist_id", "position"])

playlist_info_df.write.format("delta").mode("overwrite").save(f"{delta_path}/playlist_info")
playlist_tracks_df.write.format("delta").mode("overwrite").save(f"{delta_path}/playlist_tracks")

total_time = time() - start_time

storage_size = sum(os.path.getsize(os.path.join(root, file)) for root, dirs, files in os.walk(delta_path) for file in files)

print(f"Total time for operations (load v1, merge v2, apply update, and merge v3): {total_time:.2f} seconds")
print(f"Total space used by Delta Lake: {storage_size / (1024 * 1024):.2f} MB")

spark.stop()