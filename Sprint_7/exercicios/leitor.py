import boto3 
from datetime import datetime
import csv
s3 = boto3.client('s3')
filmes = []
series = []

with open('movies.csv', 'r', newline='') as m:
    f = csv.reader(m)
    for linhas in f:
        filmes.append(','.join(linhas))
    
with open('series.csv', 'r', newline='') as s:
    i = csv.reader(s)
    for linhas in i:
        series.append(','.join(linhas))

s3nome = "desafio0"
arquivo = "CSV"
s = "Series"
f = "Movies"
movies = "movies.csv"
series = "series.csv"
camada = "Raw"
origem = "Local"

data = datetime.now()

dia = data.day
mes = data.month
ano = data.year


uploadFilmes = f'{s3nome}/{camada}/{origem}/{arquivo}/{f}/{ano}/{mes}/{dia}/{movies}'
uploadSeries = f'{s3nome}/{camada}/{origem}/{arquivo}/{s}/{ano}/{mes}/{dia}/{series}'

s3.put_object(Bucket = s3nome, Key = uploadFilmes, Body = '\n'.join(filmes))
s3.put_object(Bucket = s3nome, Key = uploadSeries, Body = '\n'.join(series))