from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, ArrayType

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("TwitchStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.9.0") \
    .getOrCreate()

# Esquema atualizado para os dados de streams da Twitch
schema = StructType() \
    .add("id", StringType()) \
    .add("user_id", StringType()) \
    .add("user_login", StringType()) \
    .add("user_name", StringType()) \
    .add("game_id", StringType()) \
    .add("game_name", StringType()) \
    .add("type", StringType()) \
    .add("title", StringType()) \
    .add("viewer_count", IntegerType()) \
    .add("started_at", StringType()) \
    .add("language", StringType()) \
    .add("thumbnail_url", StringType()) \
    .add("tag_ids", ArrayType(StringType())) \
    .add("tags", ArrayType(StringType())) \
    .add("is_mature", BooleanType())

# Ler os dados do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitch-input") \
    .load()

# Converter JSON
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# df.printSchema()

# Escrever para o console (para debug)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
