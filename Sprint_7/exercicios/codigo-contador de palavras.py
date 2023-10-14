from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("Contagem de Palavras").getOrCreate()

arquivo_texto = spark.read.text("file:///home/jovyan/README.md")

def dividir_linha(linha):
    palavras = re.findall(r'\w+', linha.value)
    return palavras

palavras = arquivo_texto.rdd.flatMap(dividir_linha)

pares_palavra = palavras.map(lambda palavra: (palavra, 1))

contagem_palavras = pares_palavra.reduceByKey(lambda a, b: a + b)

for palavra, contagem in contagem_palavras.collect():
    print(f"{palavra}: {contagem}")
