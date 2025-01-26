from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, lit, when

# Caminhos dos arquivos
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

# Leitura dos dados
playlists_v2_df = spark.read.json(playlists_v2_path)
tracks_v2_df = spark.read.json(tracks_v2_path)

# Caminhos das tabelas Parquet
playlist_tracks_path = "datalake/silver/playlist_tracks"
playlist_info_path = "datalake/silver/playlist_info"
song_info_path = "datalake/silver/song_info"
album_info_path = "datalake/silver/album_info"
artist_info_path = "datalake/silver/artist_info"

# Leitura das tabelas Parquet
playlist_tracks_df = spark.read.parquet(playlist_tracks_path)
playlist_info_df = spark.read.parquet(playlist_info_path)
song_info_df = spark.read.parquet(song_info_path)
album_info_df = spark.read.parquet(album_info_path)
artist_info_df = spark.read.parquet(artist_info_path)

# Atualização e inserção de novos dados na playlist_tracks
new_playlist_tracks = tracks_v2_df.select(
    col("pid").alias("playlist_id"),
    col("pos").alias("position"),
    col("track_uri"),
    col("artist_uri"),
    col("album_uri"),
    col("duration_ms").alias("duration")
)

# Atualizando ou inserindo novos dados nas playlists
updated_playlist_tracks_df = playlist_tracks_df.alias("old").join(
    new_playlist_tracks.alias("new"),
    (col("old.playlist_id") == col("new.playlist_id")) & (col("old.position") == col("new.position")),
    "left"
).select(
    col("new.track_uri").alias("track_uri"),
    col("new.artist_uri").alias("artist_uri"),
    col("new.album_uri").alias("album_uri"),
    col("new.duration").alias("duration"),
    col("old.playlist_id"),
    col("old.position")
).union(
    new_playlist_tracks.filter(~new_playlist_tracks["position"].isin(playlist_tracks_df.select("position").rdd.flatMap(lambda x: x).collect()))
)

# Escrevendo os dados atualizados de playlist_tracks
updated_playlist_tracks_df.write.format("parquet").mode("overwrite").save(playlist_tracks_path)

# Atualização e inserção de novos dados na playlist_info
new_playlist_info = playlists_v2_df.select(
    col("name"),
    col("pid").alias("playlist_id"),
    col("description"),
    col("collaborative")
)

# Atualizando ou inserindo novos dados na playlist_info
updated_playlist_info_df = playlist_info_df.alias("old").join(
    new_playlist_info.alias("new"),
    col("old.playlist_id") == col("new.playlist_id"),
    "left"
).select(
    col("new.name").alias("name"),
    col("new.description").alias("description"),
    col("new.collaborative").alias("collaborative"),
    col("old.playlist_id")
)

# Escrevendo os dados atualizados de playlist_info
updated_playlist_info_df.write.format("parquet").mode("overwrite").save(playlist_info_path)

# Atualizando os dados agregados de playlist_info (Gold Layer)
updated_playlist_info = playlist_tracks_df.groupBy("playlist_id").agg(
    countDistinct("track_uri").alias("number_of_tracks"),
    countDistinct("artist_uri").alias("number_of_artists"),
    countDistinct("album_uri").alias("number_of_albums"),
    sum("duration").alias("total_duration")
).join(
    playlist_info_df,
    "playlist_id",
    "inner"
)

updated_playlist_info.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_info")

# Atualizando as faixas nas playlists (Gold Layer)
gold_playlist_tracks = playlist_tracks_df.join(
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

gold_playlist_tracks.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_tracks")

# Atualizando playlist específica (ID 11992) na camada Silver e Gold
playlist_info_df = playlist_info_df.withColumn(
    "name",
    when(col("playlist_id") == 11992, lit("GYM WORKOUT")).otherwise(col("name"))
).withColumn(
    "collaborative",
    when(col("playlist_id") == 11992, lit(True)).otherwise(col("collaborative"))
)

# Escrevendo a atualização na camada Silver
playlist_info_df.write.format("parquet").mode("overwrite").save(playlist_info_path)

# Atualizando na Gold Layer após a correção
updated_playlist_info = playlist_tracks_df.groupBy("playlist_id").agg(
    countDistinct("track_uri").alias("number_of_tracks"),
    countDistinct("artist_uri").alias("number_of_artists"),
    countDistinct("album_uri").alias("number_of_albums"),
    sum("duration").alias("total_duration")
).join(
    playlist_info_df,
    "playlist_id",
    "inner"
)

updated_playlist_info.write.format("parquet").mode("overwrite").save("datalake/gold/playlist_info")
