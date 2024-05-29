from pyspark import SparkConf
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

# conf = SparkConf()
# conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
# conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentials Provider')
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

def process(self, filename=None):
    print("main do tratamento")
    df = set_up(self, filename)

def reduzir_redundancia():
    print("reduza a redundancia aqui")