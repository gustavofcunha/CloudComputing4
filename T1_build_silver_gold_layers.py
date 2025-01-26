from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum

playlists_v1_path = '/shared/sampled/playlists_v1.json'
tracks_v1_path = '/shared/sampled/tracks_v1.json'

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
sc = spark.sparkContext

playlists_df = spark.read.json(playlists_v1_path)
tracks_df = spark.read.json(tracks_v1_path)

song_info_df = tracks_df.select(
    col("track_name").alias("name"),
    col("track_uri"),
    col("duration_ms").alias("duration"),
    col("album_uri"),
    col("artist_uri")
)

album_info_df = tracks_df.select(
    col("album_name").alias("name"),
    col("album_uri"),
    col("track_uri"),
    col("artist_uri")
).distinct()

artist_info_df = tracks_df.select(
    col("artist_name").alias("name"),
    col("artist_uri")
).distinct()

playlist_info_df = playlists_df.select(
    col("name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative")
)

playlist_tracks_df = tracks_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("album_uri"),
    col("duration_ms").alias("duration")
)

song_info_df.write.format("parquet").mode("overwrite").save("datalake/silver/song_info")
album_info_df.write.format("parquet").mode("overwrite").save("datalake/silver/album_info")
artist_info_df.write.format("parquet").mode("overwrite").save("datalake/silver/artist_info")
playlist_info_df.write.format("parquet").mode("overwrite").save("datalake/silver/playlist_info")
playlist_tracks_df.write.format("parquet").mode("overwrite").save("datalake/silver/playlist_tracks")

gold_playlist_info_df = playlist_tracks_df.groupBy("playlist_id").agg(
    countDistinct("track_uri").alias("number_of_tracks"),
    countDistinct("artist_uri").alias("number_of_artists"),
    countDistinct("album_uri").alias("number_of_albums"),
    sum("duration").alias("total_duration")
).join(playlist_info_df, "playlist_id", "inner")

gold_playlist_tracks_df = playlist_tracks_df.join(
    song_info_df.select("track_uri", col("name").alias("song_name"), "album_uri", "artist_uri"),
    "track_uri",
    "inner"
).join(
    album_info_df.select("album_uri", col("name").alias("album_name")),
    "album_uri",
    "inner"
).join(
    artist_info_df.select("artist_uri", col("name").alias("artist_name")),
    "artist_uri",
    "inner"
).select(
    "playlist_id", "position", "song_name", "album_name", "artist_name"
)

gold_playlist_info_df.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_info")
gold_playlist_tracks_df.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_tracks")

gold_playlist_info_json = "datalake/gold/playlist_info_json"
gold_playlist_info_parquet = "datalake/gold/playlist_info"

gold_playlist_info_df.write.format("json").mode("overwrite").save(gold_playlist_info_json)

def measure_load_time(path, format_):
    from time import time
    start_time = time()
    df = spark.read.format(format_).load(path)
    df.count()
    return time() - start_time

json_time = measure_load_time(gold_playlist_info_json, "json")
parquet_time = measure_load_time(gold_playlist_info_parquet, "parquet")

print(f"JSON load time: {json_time:.2f} seconds")
print(f"Parquet load time: {parquet_time:.2f} seconds")