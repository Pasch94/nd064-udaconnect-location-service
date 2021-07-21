import os

from app.udaconnect.services import setup_grpc_server, setup_kafka_consumer

def start_grpc_server():
    SERVER_PORT = os.getenv('GRPC_PORT', '[::]:5005')
    setup_grpc_server(SERVER_PORT)
    print("gRPC Server setup..")

def start_kafka_consumer():
    TOPIC_NAME = os.getenv('KAFKA_LOCATION_TOPIC', 'location')
    KAFKA_HOST = os.getenv('KAFKA_HOST', '192.168.65.4')
    KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
    setup_kafka_consumer(TOPIC_NAME, KAFKA_HOST, KAFKA_PORT)
