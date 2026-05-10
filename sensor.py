import time
import random
import json
from kafka import KafkaProducer

KAFKA_BROKER = '44.205.220.232'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sensor iniciado. Publicando leituras...")

while True:
    leitura = {
        'timestamp': time.time(),
        'temperatura': round(random.uniform(15.0, 45.0), 2)
    }
    producer.send('leituras', leitura)
    print(f"Publicado: {leitura}")
    time.sleep(3)