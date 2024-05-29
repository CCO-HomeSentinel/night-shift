import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import logger

load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


class MySQLConnection:
    def __init__(self):
        try:
            self.engine = create_engine(
                f"mysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@"
                f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
            )
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

        except Exception as e:
            logger.log("error", f'Erro ao conectar com o banco de dados. {e}')

    def get_session(self):
        return self.session

    def close_connection(self):
        self.session.close()

    def return_dict(self, obj):
        return {col.name: getattr(obj, col.name) for col in obj.__table__.columns}

    def execute_select_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            results = result.fetchall()
            return results
        
    def mapper_query(self):
        query = '''
            SELECT ss.id, ms.id, ms.tipo, ms.total_bateria
            FROM home_sentinel.sensor ss
                JOIN home_sentinel.modelo_sensor ms ON ss.modelo_sensor_id = ms.id
                ORDER BY ss.id;
        '''
        results = self.execute_select_query(query)
        return results