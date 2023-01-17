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
# Lendo dfs para pegar criacao de views
LOCAL = f'{pasta_local}/desafio2rp/silver/prescribers/*'
prescribers = (spark.read.format('parquet').load(LOCAL))

LOCAL2 = f'{pasta_local}/desafio2rp/silver/prescriptions/*'
prescriptions = (spark.read.format('parquet').load(LOCAL2))
# ------------------------------------------------------------------------------------------
# Criando Views temporárias
prescribers.createOrReplaceTempView('prescritor')
prescriptions.createOrReplaceTempView('prescricao')
# ------------------------------------------------------------------------------------------
# Crie um dataframe contendo os 10 principais produtos químicos prescritos por região.
query1 = """
SELECT a.row as posicao, a.produtos_quimicos, b.REGIONAL_OFFICE_NAME as regiao FROM(
SELECT *,ROW_NUMBER() OVER(PARTITION BY regiao ORDER BY quantidade desc) as row
FROM(
SELECT  
BNF_DESCRIPTION as produtos_quimicos,
REGIONAL_OFFICE_CODE as regiao,
count(*) as quantidade
FROM prescricao
GROUP BY 1,2
)
WHERE regiao not like '%-%'
) a
LEFT JOIN prescritor b
ON a.regiao = b.REGIONAL_OFFICE_CODE
WHERE row BETWEEN 1 AND 10
GROUP BY 1,2,3
ORDER BY 3,1
"""
df1 = spark.sql(query1)
df1.show(30, truncate=False)
# ------------------------------------------------------------------------------------------
# Quais produtos químicos prescritos tiveram a maior somatória de custos por mês ? Liste os 10 primeiros.

query2 = """
SELECT mes, row as posicao, produtos_quimicos, ROUND(custo_total,2) as custo_total FROM(
SELECT *,ROW_NUMBER() OVER(PARTITION BY mes ORDER BY custo_total desc) as row FROM(
SELECT 
MONTH as mes,
BNF_DESCRIPTION as produtos_quimicos,
SUM(NIC) as custo_total 
FROM prescricao
GROUP BY 1,2
)
)
WHERE row BETWEEN 1 AND 10
GROUP BY 1,2,3,4
ORDER BY 1,2
"""
df2 = spark.sql(query2)
df2.show(30, truncate=False)

# ------------------------------------------------------------------------------------------
# Quais são as prescrições mais comuns ?

query3 = """
SELECT  row as posicao, prescricoes FROM(
SELECT *,ROW_NUMBER() OVER(ORDER BY quatidade desc) as row FROM(
SELECT 
BNF_CHAPTER_PLUS_CODE as prescricoes,
COUNT() as quatidade 
FROM prescricao
GROUP BY 1
)
)

GROUP BY 1,2
ORDER BY 1
"""
df3 = spark.sql(query3)
df3.show(100, truncate=False)

# ------------------------------------------------------------------------------------------
# Qual produto químico é mais prescrito por cada prescriber ?

query4 = """
SELECT if(b.PRACTICE_NAME is null,'SEM IDENTIFICACAO',b.PRACTICE_NAME) as prescriber, a.produtos_quimicos,a.quatidade as quatidade_prescricoes  FROM(
SELECT *,ROW_NUMBER() OVER(PARTITION BY prescriber ORDER BY quatidade desc) as row FROM(
SELECT
PRACTICE_CODE as prescriber, 
BNF_DESCRIPTION as produtos_quimicos,
COUNT(*) as quatidade 
FROM prescricao
GROUP BY 1,2
)
) a
LEFT JOIN prescritor b
ON a.prescriber = b.PRACTICE_CODE

WHERE row = 1
GROUP BY 1,2,3
ORDER BY 3 desc
"""
df4 = spark.sql(query4)
df4.show(30, truncate=False)
# ------------------------------------------------------------------------------------------
# Quantos prescribers foram adicionados no ultimo mês ?

query5 = """
SELECT *,(total_prescribers - LAG(total_prescribers,1,total_prescribers) over( ORDER BY mes)) as prescribers_adicionados_removidos FROM(
SELECT
MONTH as mes,
COUNT (DISTINCT PRACTICE_CODE) as total_prescribers
FROM prescritor
GROUP BY 1

)
"""

df5 = spark.sql(query5)
df5.show(30, truncate=False)
# ------------------------------------------------------------------------------------------
# Quais prescribers atuam em mais de uma região ? Ordene por quantidade de regiões antendidas

query6 = """
SELECT
PRACTICE_NAME as prescribers,
COUNT(DISTINCT REGIONAL_OFFICE_CODE) as total_regioes_atendidas
FROM prescritor
GROUP BY 1
ORDER BY 2 desc

"""

df6 = spark.sql(query6)
df6.show(30, truncate=False)
# ------------------------------------------------------------------------------------------
# Qual o preço médio dos químicos prescritos em no ultimo mês coletado ?

query7 = """
SELECT
MONTH as mes,
BNF_DESCRIPTION as produtos_quimicos,
AVG(NIC) as preco_medio
FROM prescricao
WHERE MONTH in (
SELECT
max(MONTH) as mes
FROM prescricao
)
GROUP BY 1,2
"""

df7 = spark.sql(query7)
df7.show(60, truncate=False)
# ------------------------------------------------------------------------------------------
# @title Gere uma tabela que contenha apenas a prescrição de maior valor de cada usuário.

query8 = """
SELECT if(b.PRACTICE_NAME is null,'NAO IDENTIFICADO', b.PRACTICE_NAME) as prescribers,a.prescricoes,a.valor FROM(
SELECT *,ROW_NUMBER() OVER(PARTITION BY prescribers ORDER BY valor desc) as row 
FROM(
SELECT 
PRACTICE_CODE as prescribers,
BNF_CHAPTER_PLUS_CODE as prescricoes,
NIC as valor
FROM prescricao
)
) a
LEFT JOIN prescritor b
ON a.prescribers = b.PRACTICE_CODE

WHERE row = 1
GROUP BY 1,2,3
ORDER BY 3 desc
"""
df8 = spark.sql(query8)
df8.show(60, truncate=False)
