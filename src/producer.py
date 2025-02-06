from kafka import KafkaProducer
import json
import requests
import time

# Configuração do produtor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuração da API da Twitch
CLIENT_ID = ''
ACCESS_TOKEN = ''
HEADERS = {
    'Client-ID': CLIENT_ID,
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}
URL = 'https://api.twitch.tv/helix/streams?first=10'

while True:
    response = requests.get(URL, headers=HEADERS)
    if response.status_code == 200:
        data = response.json().get('data', [])
        # Criação do novo conjunto de dados com os campos necessários
        reduced_data = [
            {
                'user_name': stream.get('user_name'),
                'game_name': stream.get('game_name'),
                'viewer_count': stream.get('viewer_count'),
                'started_at': stream.get('started_at')
            }
            for stream in data
        ]
        print(reduced_data)
        # Envia todo o novo conjunto de dados para o Kafka
        producer.send('twitch-input', reduced_data)
        print(f"Enviados {len(reduced_data)} streams para o Kafka")
    time.sleep(15)  # Coleta dados a cada 15 segundos