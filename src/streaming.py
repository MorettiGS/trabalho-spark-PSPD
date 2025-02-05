from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import openai
import concurrent.futures

openai.api_key = "sua_api_key"

def process_data(data):
    prompt = (
        f"Analise os seguintes dados da Twitch sobre jogos mais jogados e gere insights "
        f"estruturados para gráficos do Kibana, incluindo estatísticas e tendências:\n\n{data}"
    )
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "system", "content": prompt}]
        )
        return response['choices'][0]['message']['content']
    
    except Exception as e:
        return f"Erro ao processar: {str(e)}"

# Paralelismo para evitar bloqueios na UDF
def parallel_udf(func):
    def wrapper(data):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(func, data)
            return future.result()
    return udf(wrapper, StringType())

# Criar UDF para processar os dados com paralelismo
process_data_udf = parallel_udf(process_data)

# Aplicar IA
df = df.withColumn("ai_analysis", process_data_udf(col("name")))

# Escrever para um novo tópico Kafka
df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "twitch-output") \
    .outputMode("append") \
    .start()
