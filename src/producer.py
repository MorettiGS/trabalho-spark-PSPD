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
        for stream in data:
            producer.send('twitch-input', stream)  # Envia os dados para o Kafka
        print(f"Enviados {len(data)} streams para o Kafka")
        print(f"Streams enviadas: {data}")
    time.sleep(10)  # Coleta dados a cada 10 segundos

