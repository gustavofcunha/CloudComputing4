from pyspark.sql import SparkSession

# Inicializar o SparkSession
spark = SparkSession.builder \
    .appName("datalake-inspect") \
    .getOrCreate()

# Função para imprimir as 10 primeiras linhas de cada tabela
def print_top_10_from_layer(layer_path, layer_name):
    print(f"\nTop 10 rows from {layer_name} layer:")
    df = spark.read.parquet(layer_path)
    df.show(10, truncate=False)

# Caminhos para as camadas (Bronze, Silver e Gold)
layers = {
    "silver": {
        "playlist_info": "datalake/silver/playlist_info",
        "playlist_tracks": "datalake/silver/playlist_tracks",
        "song_info": "datalake/silver/song_info",
        "album_info": "datalake/silver/album_info",
        "artist_info": "datalake/silver/artist_info"
    },
    "gold": {
        "playlist_info": "datalake/gold/playlist_info",
        "playlist_tracks": "datalake/gold/playlist_tracks"
    }
}

# Imprimir as 10 primeiras linhas de cada tabela da camada Silver
for table, path in layers["silver"].items():
    print_top_10_from_layer(path, f"Silver - {table}")

# Imprimir as 10 primeiras linhas de cada tabela da camada Gold
for table, path in layers["gold"].items():
    print_top_10_from_layer(path, f"Gold - {table}")

spark.stop()
