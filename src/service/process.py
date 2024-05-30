import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text


load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import logger


def initialize_spark():
    logger.log("info", "Iniciando Spark")
    global spark
    # spark = SparkSession.builder.appName("app").getOrCreate()


def initialize_database(engine_pronta):
    logger.log("info", "Iniciando conexão com o banco de dados")
    global engine
    engine = engine_pronta


def process(self, filename=None):
    logger.log("info", f"Processando arquivo {filename}")
    global connection
    connection = engine.connect()

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

    
    dados = trazer_arquivo(filename)
    dados_reduzidos = reduzir_redundancia(dados)
    tabularizar(dados_reduzidos)
    criar_kpis(dados)
    
    print("main do tratamento")


def trazer_arquivo(filename):
    logger.log("info", f"Buscando no bucket: {BUCKET_NAME}")
    print("traga o arquivo aqui")

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")


def reduzir_redundancia(dados):
    logger.log("info", "Reduzindo redundancia dos dados")
    print("reduza a redundancia aqui")

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

def tabularizar(dados):
    logger.log("info", f"Realizando carga de dados no banco de dados: {MYSQL_DATABASE}")
    print("tabularize os dados aqui")

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

def criar_kpis(dados):
    logger.log("info", f"Realizando carga de dados de KPIs no banco de dados: {MYSQL_DATABASE}")
    print("crie os kpis aqui")

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

    connection.close()
