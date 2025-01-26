from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, when

playlists_v2_path = '/shared/sampled/playlists_v2.json'
tracks_v2_path = '/shared/sampled/tracks_v2.json'

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

playlists_v1_df = spark.read.json('/shared/sampled/playlists_v1.json')
tracks_v1_df = spark.read.json('/shared/sampled/tracks_v1.json')
playlists_v2_df = spark.read.json(playlists_v2_path)
tracks_v2_df = spark.read.json(tracks_v2_path)

playlist_info_df = spark.read.parquet("datalake/silver/playlist_info")
playlist_tracks_df = spark.read.parquet("datalake/silver/playlist_tracks")
song_info_df = spark.read.parquet("datalake/silver/song_info")
album_info_df = spark.read.parquet("datalake/silver/album_info")
artist_info_df = spark.read.parquet("datalake/silver/artist_info")

new_playlists_df = playlists_v2_df.select(
    col("name").alias("playlist_name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative")
)

playlist_info_df = playlist_info_df.unionByName(new_playlists_df, allowMissingColumns=True).dropDuplicates(["playlist_id"])

update_playlist_df = spark.createDataFrame([
    (11992, "GYM WORKOUT", True)
], ["playlist_id", "playlist_name", "collaborative"])

playlist_info_df = playlist_info_df.alias("playlist_info").join(update_playlist_df.alias("update_playlist"), "playlist_id", "left").select(
    "playlist_info.playlist_id",
    when(col("update_playlist.playlist_name").isNotNull(), col("update_playlist.playlist_name")).otherwise(col("playlist_info.name")).alias("name"),
    when(col("update_playlist.collaborative").isNotNull(), col("update_playlist.collaborative").cast("boolean")).otherwise(col("playlist_info.collaborative").cast("boolean")).alias("collaborative"),
    "playlist_info.description"
)

new_tracks_df = tracks_v2_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("album_uri"),
    col("duration_ms").alias("duration")
)

playlist_tracks_df = playlist_tracks_df.alias("old").join(new_tracks_df.alias("new"), 
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


new_song_info_df = tracks_v2_df.select(
    col("track_name"),
    col("track_uri"),
    col("duration_ms").alias("duration"),
    col("album_uri"),
    col("artist_uri")
)

new_album_info_df = tracks_v2_df.select(
    col("album_name"),
    col("album_uri"),
    col("track_uri"),
    col("artist_uri")
).distinct()

new_artist_info_df = tracks_v2_df.select(
    col("artist_name"),
    col("artist_uri")
).distinct()

song_info_df = song_info_df.unionByName(new_song_info_df, allowMissingColumns=True).dropDuplicates(["track_uri"])
album_info_df = album_info_df.unionByName(new_album_info_df, allowMissingColumns=True).dropDuplicates(["album_uri"])
artist_info_df = artist_info_df.unionByName(new_artist_info_df, allowMissingColumns=True).dropDuplicates(["artist_uri"])


playlist_info_df.write.format("delta").mode("overwrite").save("datalake/silver/playlist_info")
playlist_tracks_df.write.format("delta").mode("overwrite").save("datalake/silver/playlist_tracks")
song_info_df.write.format("delta").mode("overwrite").save("datalake/silver/song_info")
album_info_df.write.format("delta").mode("overwrite").save("datalake/silver/album_info")
artist_info_df.write.format("delta").mode("overwrite").save("datalake/silver/artist_info")


gold_playlist_info_df = playlist_tracks_df.groupBy("playlist_id").agg(
    countDistinct("track_uri").alias("number_of_tracks"),
    countDistinct("artist_uri").alias("number_of_artists"),
    countDistinct("album_uri").alias("number_of_albums"),
    sum("duration").alias("total_duration")
).join(playlist_info_df, "playlist_id", "inner")

gold_playlist_tracks_df = playlist_tracks_df.join(
    song_info_df.select("track_uri", "track_name", "album_uri", "artist_uri"),
    "track_uri",
    "inner"
).join(
    album_info_df.select("album_uri", "album_name"),
    "album_uri",
    "inner"
).join(
    artist_info_df.select("artist_uri", "artist_name"),
    "artist_uri",
    "inner"
).select(
    "playlist_id", "position", "track_name", "album_name", "artist_name"
)

gold_playlist_info_df.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_info")
gold_playlist_tracks_df.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_tracks")

spark.stop()
