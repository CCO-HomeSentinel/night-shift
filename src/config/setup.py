import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.logger import Logger
from connection.MySQLConnection import MySQLConnection
from api.app import create_app
from service.process import initialize_spark

load_dotenv()

def setup_database():
    mysql_connection = MySQLConnection()
    session = mysql_connection.get_session()
    relacoes = mysql_connection.mapper_query()
    session.close()

    return relacoes


def setup_spark():
    spark = initialize_spark()
    return spark


def setup_api():
    app = create_app()
    return app