import os
import sys
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import Logger
from connection.MySQLConnection import MySQLConnection
from service.process import initialize_spark, process

load_dotenv()

ENABLE_LOGS = os.getenv("ENABLE_LOGS").lower() == "true"

if ENABLE_LOGS:
    logger = Logger()
else:
    logger = None

def create_app():
    app = Flask(__name__)

    @app.route('/api/v1/file', methods=['POST'])
    def handle_file():
        data = request.get_json()

        if 'name' in data:
            name = data['name']

            thread = threading.Thread(target=process, args=(name, logger,))
            thread.start()

            if logger:
                logger.info(f"Requisição recebida e em processamento. Nome: {name}")

            return jsonify({"message": "Requisição recebida e em processamento."}), 202
        else:
            if logger:
                logger.error("Não encontrado chave 'name' em body")

            return jsonify({"error": "Não encontrado chave 'name' em body"}), 400

    return app