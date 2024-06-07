import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from connection.MySQLConnection import MySQLConnection
from api.app import create_app
from service.process import initialize_spark, initialize_database

load_dotenv()

# def setup_database():
#     mysql_connection = MySQLConnection()

#     initialize_database(mysql_connection.engine)


def setup_spark():
    initialize_spark()


def setup_api():
    app = create_app()
    return app