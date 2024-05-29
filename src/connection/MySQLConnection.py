import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()


class MySQLConnection:
    def __init__(self):
        try:
            self.engine = create_engine(
                f"mysql://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}@"
                f"{os.getenv('MYSQL_HOST')}:{int(os.getenv('MYSQL_PORT'))}/{os.getenv('MYSQL_DATABASE')}"
            )
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

        except Exception as e:
            print(f'Erro ao conectar com o banco de dados. {e}')

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