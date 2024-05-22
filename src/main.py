import os
import time
from dotenv import load_dotenv
import schedule

load_dotenv()

HORA_TURNO = os.getenv('HORA_TURNO')
MINUTO_TURNO = os.getenv('MINUTO_TURNO')

def task():
    print("Chamar código existente")

def main():
    horario = f"{HORA_TURNO}:{MINUTO_TURNO}"
    schedule.every().day.at(horario).do(task)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()