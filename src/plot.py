from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# Configuração do consumidor Kafka
consumer = KafkaConsumer(
    'twitch-output',  # Nome do tópico Kafka
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Desserializa a mensagem JSON
)

# Função para gerar gráfico de pizza
def plot_pie_chart(title, labels, values, top_n=10):
    # Ordenar por valor e pegar os top N elementos
    sorted_data = sorted(zip(values, labels), reverse=True)[:top_n]
    values, labels = zip(*sorted_data)

    # Criar cores personalizadas
    colors = plt.cm.Paired.colors[:len(labels)]  # Paleta de cores

    # Criar o gráfico
    plt.figure(figsize=(8, 8))
    wedges, texts, autotexts = plt.pie(
        values, labels=None, autopct='%1.1f%%', startangle=140,
        colors=colors, wedgeprops={'edgecolor': 'black'}
    )

    # Melhorando a aparência
    plt.title(title, fontsize=14, fontweight="bold", pad=20)
    plt.legend(wedges, labels, title="Categorias", loc="center left", bbox_to_anchor=(1, 0.5))
    plt.tight_layout()
    
    # Mostrar o gráfico
    plt.show()

# Função para processar os dados do Kafka e gerar gráficos
def process_and_plot_data():
    for message in consumer:
        data = message.value  # Dados recebidos do Kafka

        # print(data)

        if 'ai_analysis' in data:
            ai_analysis_str = data['ai_analysis']
            # Remover a parte de código e parsear o JSON
            try:
                # Extrair o conteúdo JSON dentro do código
                ai_analysis_json = json.loads(ai_analysis_str.strip('```json\n').strip('```'))
                
                print(ai_analysis_json.keys())

                # Verificar se as chaves necessárias estão presentes
                if 'games' in ai_analysis_json and 'streamers' in ai_analysis_json:
                    # Gráfico de jogos mais assistidos
                    games = ai_analysis_json["games"]
                    game_labels = [game["name"] for game in games]
                    game_values = [game["count"] for game in games]
                    plot_pie_chart("Top Jogos por Viewers", game_labels, game_values)

                    # Gráfico de streamers mais assistidos
                    streamers = ai_analysis_json["streamers"]
                    streamer_labels = [streamer["name"] for streamer in streamers]
                    streamer_values = [streamer["count"] for streamer in streamers]
                    plot_pie_chart("Top Streamers por Viewers", streamer_labels, streamer_values)
                else:
                    print("Formato de dados inválido dentro de ai_analysis:", ai_analysis_json)

            except json.JSONDecodeError as e:
                print(f"Erro ao processar JSON: {str(e)}")
        else:
            print("Campo 'ai_analysis' não encontrado nos dados:", data)

# Iniciar o processamento e plotagem dos dados
process_and_plot_data()
