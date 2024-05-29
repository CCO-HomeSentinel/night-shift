import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import threading
from functools import partial
from src.config.logger import Logger
from connection.MySQLConnection import MySQLConnection
from service.process import initialize_spark, process

load_dotenv()

ENABLE_LOGS = os.getenv("ENABLE_LOGS").lower() == "true"

if ENABLE_LOGS:
    logger = Logger()
else:
    logger = None

INTERVALO_BACKUP = int(os.getenv("INTERVALO_BACKUP", 1))


def set_up_database():
    mysql_connection = MySQLConnection()
    session = mysql_connection.get_session()
    relacoes = mysql_connection.mapper_query()
    session.close()

    return relacoes


def set_up_spark():
    spark = initialize_spark(logger)
    return spark


def print_name(name):
    if logger:
        logger.info(f"Received name: {name}")
    else:
        print(f"Received name: {name}")


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

        return jsonify({"message": "Requisição recebi e em processamento."}), 202
    else:
        if logger:
            logger.error("Não encontrado chave 'name' em body")

        return jsonify({"error": "Não encontrado chave 'name' em body"}), 400


def main():
    relacoes = set_up_database()
    spark = set_up_spark()

    app.run(debug=True)

if __name__ == '__main__':
    main()