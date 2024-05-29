import logging
import os
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

INTERVALO_BACKUP_LOGGER = os.getenv("INTERVALO_BACKUP_LOGGER")

path_atual = os.path.dirname(os.path.abspath(__file__))
dois_diretorios_acima = os.path.dirname(os.path.dirname(path_atual))


class Logger:

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"logs_{current_date}.log"
        log_file = os.path.join(dois_diretorios_acima, "logs", filename)
        path = os.path.dirname(log_file)

        if not os.path.exists(path):
            os.makedirs(path)

        handler = TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=INTERVALO_BACKUP_LOGGER
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)


    def get_logger(self):
        return self.logger
