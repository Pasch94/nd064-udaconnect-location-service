from flask import Flask, jsonify
from flask_cors import CORS
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

app = Flask(__name__)

def create_app(env=None):
    from app.config import config_by_name
    from app.udaconnect import start_grpc_server
    from app.udaconnect import start_kafka_consumer

    app.config.from_object(config_by_name[env or "test"])
    api = Api(app, title="UdaConnect API", version="0.1.0")

    CORS(app)  # Set CORS for development

    db.init_app(app)
    with app.app_context():
        db.create_all()

    start_grpc_server()
    start_kafka_consumer()

    @app.route("/health")
    def health():
        return jsonify("healthy")

    return app
