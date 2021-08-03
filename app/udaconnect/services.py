import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

import grpc
import app.udaconnect.proto.location_pb2 as loc_pb2
import app.udaconnect.proto.location_pb2_grpc as loc_pb2_grpc

from app import db, app
from .models import Location
from .schemas import LocationSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text

from kafka import KafkaConsumer

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("location-service")

class LocationService(loc_pb2_grpc.LocationServiceServicer):

    def retrieve(self, location_id) -> Location:
        with app.app_context():
            location, coord_text = (
                db.session.query(Location, Location.coordinate.ST_AsText())
                .filter(Location.id == location_id)
                .one()
            )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return loc_pb2.Location(id=location_id,
                                person_id=location.person_id,
                                creation_time=datetime.timestamp(location.creation_time),
                                longitude=float(location.longitude),
                                latitude=float(location.latitude))

    def create_from_kafka(self, location: Dict) -> Location:
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        new_location = Location()
        new_location.person_id = location["person_id"]
        new_location.creation_time = location["creation_time"]
        new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
        db.session.add(new_location)
        db.session.commit()

        return new_location

    # gRPC function
    def Get(self, request, context):
       return self.retrieve(request.id)


def setup_grpc_server(server_port):
    server = grpc.server(ThreadPoolExecutor(max_workers=5))
    loc_pb2_grpc.add_LocationServiceServicer_to_server(LocationService(), server)
    server.add_insecure_port(server_port)

    def serve(server):
        server.start()
        logger.debug('gRPC server added')
        server.wait_for_termination()

    serve_thread = threading.Thread(target=serve, args=(server,))
    serve_thread.start()

def setup_kafka_consumer(topic_name, server, port):
    consuming = True
    kafka_server = ':'.join([server, port])
    print('Connecting to kafka server {}'.format(kafka_server))
    consumer = KafkaConsumer(topic_name, bootstrap_servers=[kafka_server], api_version=(2, 8, 0))

    def consume(consumer):
        while consuming:
            messages = consumer.poll()
            for message in messages:
                LocationService().create_from_kafka(message)

    # Thread does not stop
    consume_thread = threading.Thread(target=consume, args=(consumer,))
    consume_thread.start()
    logger.debug('Kafka consumer thread started')

