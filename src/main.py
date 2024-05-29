from config.setup import setup_database, setup_spark, setup_api
import os
from dotenv import load_dotenv

load_dotenv()
PORT = int(os.getenv("FLASK_PORT", 80))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"


def main():
    relacoes = setup_database()
    spark = setup_spark()
    app = setup_api()
    app.run(debug=FLASK_DEBUG, port=PORT)


if __name__ == '__main__':
    main()
