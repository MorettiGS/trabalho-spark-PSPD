from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, collect_list, col, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, ArrayType
from google import genai

# Criar sessão Spark otimizada
spark = SparkSession.builder \
    .appName("TwitchProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.9.0") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Esquema dos dados da Twitch
schema = StructType() \
    .add("user_name", StringType()) \
    .add("game_name", StringType()) \
    .add("viewer_count", IntegerType()) \
    .add("started_at", StringType())

# Ler os dados do Kafka com checkpointing para evitar perda de dados
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitch-input") \
    .option("startingOffsets", "earliest") \
    .load()

# Converter JSON
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Criar conexão com Google Generative AI
client = genai.Client(api_key="")

def process_all_data(data_list):
    if not data_list:
        return "Nenhum dado recebido para análise."

    print(f"TODOS OS DADOS DO STREAM: {data_list}")

    prompt = (
        "Analise os seguintes dados da Twitch, em formato JSON, sobre as streams mais famosas e gere insights "
        "sobre os jogos mais jogados e os streamers mais famosos. "
        "Certifique-se de que a sua resposta é composta de APENAS o texto de um arquivo JSON em código puro (sem caracteres '\n', sem espaços, etc) "
        "incluindo os dados em uma estrutura que possa ser traduzida para 2 gráficos contendo essas 2 informações."
        "Faça o JSON resultado especificamente com as chaves 'games' para os jogos e 'streamers' para os streamers mais famosos."
        "
        
        "
        "Aqui estão os dados, em formato JSON:\n\n"
        f"{data_list}"
    )

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        print(f"RESPOSTA DA IA:\n\n {response.text}")
        return response.text

    except Exception as e:
        print(f"pegou excecao {str(e)}")
        return f"Erro ao processar: {str(e)}"

# Criar UDF para processar todos os dados juntos
process_all_data_udf = udf(process_all_data, StringType())

# Coletar todos os dados do stream em um único JSON (array de objetos)
df_aggregated = df.select(to_json(collect_list(struct(*df.columns))).alias("json_data"))

# Aplicar a IA sobre todos os dados agregados
df_result = df_aggregated.withColumn("ai_analysis", process_all_data_udf(col("json_data")))

# Enviar resultado único para o Kafka
query = df_result.select("ai_analysis").selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "twitch-output") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/twitch-stream") \
    .outputMode("complete") \
    .start()

query.awaitTermination()