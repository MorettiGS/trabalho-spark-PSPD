from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import google.generativeai as genai
from consumer import df

genai.configure(api_key="")
model = genai.GenerativeModel("gemini-1.5-flash")

def process_data(data):
    print(f"DATA DA COLUNA: {data}")

    prompt = (
        f"Analise os seguintes dados da Twitch sobre jogos mais jogados e gere insights "
        f"estruturados para gráficos do Kibana, incluindo estatísticas e tendências:\n\n{data}"
    )
    
    try:
        response = model.generate_content(prompt)
        print(f"RESPOSTA DO GEMINI:\n\n {response}")
        return response.text
    
    except Exception as e:
        return f"Erro ao processar: {str(e)}"


process_data_udf = udf(process_data, StringType())

# Aplicar IA
df = df.withColumn("ai_analysis", process_data_udf(col("name")))

# Escrever para um novo tópico Kafka
query = df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "twitch-output") \
    .outputMode("append") \
    .start()

query.awaitTermination()

