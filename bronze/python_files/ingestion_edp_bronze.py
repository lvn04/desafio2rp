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
LOCAL = f'{pasta_local}/desafio2rp/bronze/english-prescribing-data-epd/*'
df = (spark.read.format('parquet').load(LOCAL))
data_delta = df.select(max('DATA_LOAD')).collect()[0][0]

# ------------------------------------------------------------------------------------------
# Setando data inicial e data final

# prim_ds = date(2022,9,1)
prim_ds = data_delta + relativedelta(months=1)

# ult_ds = date(2022,10,1)
ult_ds = (date.today()).replace(day=1)

# ------------------------------------------------------------------------------------------

while prim_ds < ult_ds:
    data = []
    columns_df = []
    data_df = []

    data_referencia = prim_ds
    mes_ano = data_referencia.strftime('%m/%Y')
    data_ref = data_referencia.strftime('%Y%m')
    data_load = data_referencia.strftime('%Y-%m-%d')

    # link para acessar os dados da API
    url = f"https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_{data_ref}&sql=SELECT * from `EPD_{data_ref}` limit 30000"

    # Pegando dados com requests
    req = requests.get(url)

    if req.status_code == 200:
        data = req.content
        data_edp = json.loads(data)
        print(f'Solicitação OK - "EPD_{data_ref}"')

    elif req.status_code == 404:
        print(f'Dataset "EPD_{data_ref}" não encontrado')
        break

    else:
        print(f'Erro de solicitação - "EPD_{data_ref}"')
        break

    if data_edp['success'] == True:

       # Separando os dados das colunas
        for reg in data_edp['result']['result']['records']:
            data_df.append(tuple(reg.values()))
            columns_df.append(tuple(reg.keys()))

        columns_df = set(columns_df)

        list_schema = []
        for conj_column in columns_df:
            for column in conj_column:
                list_schema.append(StructField(
                    f"{column}", StringType(), True))

        schema = StructType(list_schema)
      # ------------------------------------------------------------------------------------------
        # Criando DataFrame
        df_edp = spark.createDataFrame(data=data_df, schema=schema)

      # ------------------------------------------------------------------------------------------

        # Criando colunas de partição e data de carga
        df_edp = (df_edp.withColumn('PART', col('YEAR_MONTH'))
                  .withColumn('DATA_LOAD', lit(data_load).cast('date'))
                  )

      # ------------------------------------------------------------------------------------------
        # Selecionando as colunas

        df_edp = (
            df_edp.select(
                'BNF_CODE',
                'TOTAL_QUANTITY',
                'POSTCODE',
                'YEAR_MONTH',
                'UNIDENTIFIED',
                'PRACTICE_NAME',
                'ICB_NAME',
                'BNF_CHAPTER_PLUS_CODE',
                'ICB_CODE',
                'ACTUAL_COST',
                'QUANTITY',
                'REGIONAL_OFFICE_CODE',
                'ITEMS',
                'ADDRESS_4',
                'ADDRESS_1',
                'ADDRESS_2',
                'ADDRESS_3',
                'BNF_CHEMICAL_SUBSTANCE',
                'ADQUSAGE',
                'PCO_CODE',
                'REGIONAL_OFFICE_NAME',
                'NIC',
                'CHEMICAL_SUBSTANCE_BNF_DESCR',
                'PRACTICE_CODE',
                'PCO_NAME',
                'BNF_DESCRIPTION',
                'DATA_LOAD',
                'PART'
            )
        )

      # ------------------------------------------------------------------------------------------
      # Escrevendo dados na bronze

        DEFAULT_FOLDER = f'{pasta_local}/desafio2rp/bronze/english-prescribing-data-epd/'

        LOCATION = f'{DEFAULT_FOLDER}'

        (df_edp
         .write
         .partitionBy("part")
         .mode("append")
         # .mode('overwrite')
         .parquet(LOCATION)
         )

        print(f'Dados de {mes_ano} OK')
        prim_ds += relativedelta(months=1)
        # break
    else:
        print('Erro ao pegar os dados')
        break
