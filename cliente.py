import grpc
import temperatura_pb2
import temperatura_pb2_grpc

SERVICO_HOST = '54.237.79.123:50051'

channel = grpc.insecure_channel(SERVICO_HOST)
stub = temperatura_pb2_grpc.TemperaturaServiceStub(channel)

print("=== Última média ===")
ultima = stub.GetUltimaMedia(temperatura_pb2.Empty())
print(f"Timestamp: {ultima.timestamp}")
print(f"Média: {ultima.media}°C")
print(f"Total leituras: {ultima.total_leituras}")

print("\n=== Histórico completo ===")
historico = stub.GetHistorico(temperatura_pb2.Empty())
for m in historico.medias:
    print(f"  {m.timestamp} | {m.media}°C | {m.total_leituras} leituras")