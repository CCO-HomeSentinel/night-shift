from pyspark import SparkConf
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

def initialize_spark():
    # logger.info("Iniciando Spark")
    return 'objeto spark'

def process(self, filename=None, logger=None):
    if logger:
        logger.info(f"Processando arquivo {filename}")

    dados = trazer_arquivo(filename)
    dados_reduzidos = reduzir_redundancia(dados)
    tabularizar(dados_reduzidos)
    criar_kpis(dados)
    
    print("main do tratamento")


def trazer_arquivo(filename):
    print("traga o arquivo aqui")


def reduzir_redundancia():
    print("reduza a redundancia aqui")


def tabularizar(dados):
    print("tabularize os dados aqui")


def criar_kpis():
    print("crie os kpis aqui")
