import pyspark
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.sql.functions import to_timestamp, to_date, date_format
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import SparkContext

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date
import time
import requests
import json
import os

# Iniciando spark
spark = SparkSession.builder.appName("minhaAplicacao").getOrCreate()

# ------------------------------------------------------------------------------------------
# Pegando pasta local de trabalho
pasta_local = os.getcwd()
# ------------------------------------------------------------------------------------------
# Lendo df para pegar ultima data de carga
LOCAL = f'{pasta_local}/desafio2rp/silver/prescriptions/*'
df = (spark.read.format('parquet').load(LOCAL))
data_delta = df.select(max('MONTH')).collect()[0][0]
print(f'Ultima data de carga - {data_delta}')

# ------------------------------------------------------------------------------------------
# Lendo df da bronze e filtrando dados maior que ultima data referencia
LOCAL = f'{pasta_local}/desafio2rp/bronze/english-prescribing-data-epd/*'
edp_brz = (spark.read.format('parquet').load(
    LOCAL).filter(col('DATA_LOAD') > data_delta))
print(f'Pegando dados da bronze')

if edp_brz.count() > 0:
    # ------------------------------------------------------------------------------------------
    # Tratando colunas e montando DF de prescriptions
    prescriptions_slv = (edp_brz
                         .withColumn('TOTAL_QUANTITY', col('TOTAL_QUANTITY').cast('int'))
                         .withColumn('QUANTITY', col('QUANTITY').cast('int'))
                         .withColumn('ADQUSAGE', col('ADQUSAGE').cast('double'))
                         .withColumn('NIC', col('NIC').cast('double'))
                         .withColumn('ACTUAL_COST', col('ACTUAL_COST').cast('double'))
                         .selectExpr(
                             'DATA_LOAD as MONTH',
                             'BNF_CODE',
                             'BNF_CHEMICAL_SUBSTANCE',
                             'CHEMICAL_SUBSTANCE_BNF_DESCR',
                             'BNF_DESCRIPTION',
                             'BNF_CHAPTER_PLUS_CODE',
                             'QUANTITY',
                             'TOTAL_QUANTITY',
                             'ITEMS',
                             'ADQUSAGE',
                             'NIC',
                             'ACTUAL_COST',
                             'REGIONAL_OFFICE_CODE',
                             'PRACTICE_CODE'
                         )
                         )
    print(f'DF criado')
    # ------------------------------------------------------------------------------------------
    # Criando colunas de partição
    prescriptions_slv = (prescriptions_slv.withColumn(
        'PART', date_format(col('MONTH'), 'yyyyMM')))

    # ------------------------------------------------------------------------------------------
    # Escrevendo dados na silver

    DEFAULT_FOLDER = f'{pasta_local}/desafio2rp/silver/prescriptions/'

    LOCATION = f'{DEFAULT_FOLDER}'

    (prescriptions_slv
     .write
     .partitionBy("PART")
     .mode("append")
     # .mode('overwrite')
     .parquet(LOCATION)
     )
    print(f'Dados escritos na silver')
else:
    print(f'Não ha novos dados')
