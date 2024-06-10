import os
import sys
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import logger
from api.config.auth import token_required
from service.process import process

load_dotenv()

FLASK_ENDPOINT = os.getenv("FLASK_ENDPOINT")

def create_app():
    app = Flask(__name__)

    @app.route(FLASK_ENDPOINT, methods=['POST'])
    @token_required
    def synchronize(current_user):
        data = request.get_json()

        if 'name' in data:
            name = data['name']

            thread = threading.Thread(target=process, args=(name,))
            thread.start()

            message = f"Requisição recebida e em processamento. Nome: {name}"
            logger.log("info", message)
            return accepted(message)
        else:
            message = "Não encontrado chave 'name' em body"
            logger.log("error", message)
            return bad_request("Não encontrado chave 'name' em body")

    return app


def accepted(message):
    return jsonify({"message": message}), 202


def bad_request(message):
    return jsonify({"error": message}), 400