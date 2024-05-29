import os
from src.config.logger import Logger
from dotenv import load_dotenv
from connection.MySQLConnection import MySQLConnection
from service.process import process
from functools import partial

load_dotenv()

ENABLE_LOGS = os.getenv("ENABLE_LOGS").lower() == "true"

if ENABLE_LOGS:
    logger = Logger()

if os.getenv("INTERVALO_BACKUP") is None:
    INTERVALO_BACKUP = 1

def set_up_database():
    mysql_connection = MySQLConnection()
    session = mysql_connection.get_session()
    relacoes = mysql_connection.mapper_query()
    session.close()

    return partial(process, relacoes)

def main():
    set_up_database(logger)
    

if __name__ == '__main__':
    main()