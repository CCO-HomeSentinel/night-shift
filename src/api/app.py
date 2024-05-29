import os
import sys
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import Logger
from connection.MySQLConnection import MySQLConnection
from service.process import process

load_dotenv()

ENABLE_LOGS = os.getenv("ENABLE_LOGS").lower() == "true"
FLASK_ENDPOINT = os.getenv("FLASK_ENDPOINT")

if ENABLE_LOGS:
    logger = Logger()
else:
    logger = None

def create_app():
    app = Flask(__name__)

    @app.route(FLASK_ENDPOINT, methods=['POST'])
    def synchronize():
        data = request.get_json()

        if 'name' in data:
            name = data['name']

            thread = threading.Thread(target=process, args=(name, logger,))
            thread.start()

            message = f"Requisição recebida e em processamento. Nome: {name}"
            logger.info(message)
            return accepted(message)
        else:
            message = "Não encontrado chave 'name' em body"
            logger.error(message)
            return bad_request("Não encontrado chave 'name' em body")

    return app

def accepted(message):
    return jsonify({"message": message}), 202

def bad_request(message):
    return jsonify({"error": message}), 400