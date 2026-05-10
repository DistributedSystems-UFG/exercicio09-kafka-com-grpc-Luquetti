import grpc
import json
import time
from concurrent import futures
from kafka import KafkaConsumer
import temperatura_pb2
import temperatura_pb2_grpc
import threading

KAFKA_BROKER = '44.205.220.232'

historico = []  # armazena as médias em memória

class TemperaturaServicer(temperatura_pb2_grpc.TemperaturaServiceServicer):
    def GetUltimaMedia(self, request, context):
        if not historico:
            return temperatura_pb2.MediaTemperatura(
                timestamp="sem dados",
                media=0.0,
                total_leituras=0
            )
        ultima = historico[-1]
        return temperatura_pb2.MediaTemperatura(
            timestamp=str(ultima['timestamp']),
            media=ultima['media'],
            total_leituras=ultima['total_leituras']
        )

    def GetHistorico(self, request, context):
        medias = [
            temperatura_pb2.MediaTemperatura(
                timestamp=str(m['timestamp']),
                media=m['media'],
                total_leituras=m['total_leituras']
            ) for m in historico
        ]
        return temperatura_pb2.ListaMedias(medias=medias)

def consumir_kafka():
    consumer = KafkaConsumer(
        'medias',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("Consumindo tópico 'medias'...")
    for msg in consumer:
        historico.append(msg.value)
        print(f"Armazenado: {msg.value}")
thread = threading.Thread(target=consumir_kafka, daemon=True)
thread.start()

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
temperatura_pb2_grpc.add_TemperaturaServiceServicer_to_server(TemperaturaServicer(), server)
server.add_insecure_port('[::]:50051')
server.start()
print("Servidor gRPC rodando na porta 50051...")
server.wait_for_termination()