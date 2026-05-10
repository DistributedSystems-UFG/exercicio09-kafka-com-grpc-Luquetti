import json
import time
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER = '44.205.220.232'

consumer = KafkaConsumer(
    'leituras',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

janela = []  # leituras das últimas 2 horas
JANELA_SEGUNDOS = 7200  # 2 horas

print("Processador iniciado. Aguardando leituras...")

for msg in consumer:
    leitura = msg.value
    agora = time.time()

    janela.append(leitura)

    # remove leituras mais antigas que 2 horas
    janela = [l for l in janela if agora - l['timestamp'] <= JANELA_SEGUNDOS]

    media = round(sum(l['temperatura'] for l in janela) / len(janela), 2)

    resultado = {
        'timestamp': agora,
        'media': media,
        'total_leituras': len(janela)
    }

    producer.send('medias', resultado)
    print(f"Media calculada: {resultado}")