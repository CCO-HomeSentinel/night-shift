import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text


load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.logger import logger
from connection.MySQLConnection import MySQLConnection

global dict_baterias 

global conn_hs 
conn_hs = MySQLConnection("home_sentinel")

global conn_gold
conn_gold = MySQLConnection("gold")


def initialize_spark():
    logger.log("info", "Iniciando Spark")
    global spark
    
    spark = SparkSession.builder.master("local[*]").appName("app").getOrCreate()


def initialize_database(engine_pronta):
    logger.log("info", "Iniciando conexão com o banco de dados")
    global engine
    engine = engine_pronta


def process(filename=None):
    logger.log("info", f"Processando arquivo {filename}")

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

    
    dados = trazer_arquivo(filename)
    dados_reduzidos = reduzir_redundancia(dados)
    tabularizar(dados_reduzidos)
    
    print("main do tratamento")


def trazer_arquivo(filename):
    logger.log("info", f"Buscando no bucket: {BUCKET_NAME}")
    print("traga o arquivo aqui")
    
    #le o arquivo no bucket s3
    df = pd.read_json(f"s3a://hs-s3-bronze-dev/{filename}")
    
    #transforma o json em dados tabulares e depois em um df pandas
    df_exploded = df.withColumn("reading", F.explode("registros"))
    df_tabular = df_exploded.select("reading.*")
    df_show = df_tabular.toPandas()

    return df_show

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")


def reduzir_redundancia(dados):
    logger.log("info", "Reduzindo redundancia dos dados")
    print("reduza a redundancia aqui")
    
    #tratamento das colunas do dataframe
    df_reduzido = dados
    df_reduzido['timestamp'] = pd.to_datetime(df_reduzido['timestamp'])
    df_reduzido['valor'] = df_reduzido['valor'].replace({'false': 0, 'true': 1})
    df_reduzido['valor'] = pd.to_numeric(df_reduzido['valor'])

    #agrupando o df por sensor e retirando a média do valor
    grouped = df_reduzido.groupby(['sensor_id', pd.Grouper(key='timestamp', freq='H')])
    df_resampled = grouped['valor'].apply(calculate_statistic).reset_index()
    
    #criando um df com as informações de bateria
    df_baterias_resampled = grouped['bateria'].apply(calculate_statistic).reset_index()

    #unindo informações de bateria no df final
    df_resampled['bateria'] = df_baterias_resampled['bateria']

    #realizando um select no banco para obter as informações de bateria e calcular a porcentagem do sensor
    query = "select sensor.id, sensor.modelo_sensor_id, modelo_sensor.total_bateria from sensor join modelo_sensor on modelo_sensor.id = sensor.modelo_sensor_id;"

    resultado = conn_hs.execute(text(query))
    baterias = resultado.mappings().all()
    
    dict_baterias = {}

    for i in range(len(baterias)):
        dict_baterias[i+1] = baterias[i]['total_bateria']
    
    #criando a coluna porcentagem bateria e realizando os calculos
    df_resampled['porcentagem_bateria'] = df_resampled.apply(calcular_porcentagem_bateria, axis=1)

    return df_resampled

    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

def tabularizar(dados):
    logger.log("info", f"Realizando carga de dados no banco de dados: {MYSQL_DATABASE}")
    print("tabularize os dados aqui")
    
    dados.to_sql(con = conn_gold.get_engine(), if_exists='append', name="registro_hora")
    
    # É possível reutilizar o spark por aqui, basta chamar spark.metodo()
    # ou também o banco via connection.execute(text"SELECT * FROM tabela")

def calculate_statistic(group):
    if group.name[0] in [3, 5]:
        return group.mode()[0] if not group.mode().empty else np.nan
    else:
        return group.mean()
    
def apply_battery_value(group):
    return group['bateria'].iloc[0]


def calcular_porcentagem_bateria(row):
    sensor_id = row['sensor_id']
    valor_atual = row['bateria']
    valor_maximo = dict_baterias.get(sensor_id)
    
    return (valor_atual / valor_maximo) * 100