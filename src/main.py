import os
import time
from dotenv import load_dotenv
import schedule
from connection.MySQLConnection import MySQLConnection

load_dotenv()

HORA_TURNO = os.getenv('HORA_TURNO')
MINUTO_TURNO = os.getenv('MINUTO_TURNO')
TEMPO_ATUALIZACAO = int(os.getenv('TEMPO_ATUALIZACAO'))

def set_up():
    mysql_connection = MySQLConnection()
    session = mysql_connection.get_session()
    # dados a serem catalogados
    session.close()

def task():
    print("Chamar código existente")

def main():
    set_up()
    horario = f"{HORA_TURNO}:{MINUTO_TURNO}"
    schedule.every().day.at(horario).do(task)

    while True:
        schedule.run_pending()
        time.sleep(TEMPO_ATUALIZACAO)

if __name__ == '__main__':
    main()