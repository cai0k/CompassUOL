import pandas as pd
from datetime import datetime
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    origem = "s3"
    s3nome = "desafio0"
    formato = "JSON"
    f = "Movies"
    filmes = "filmes.json"
    filmes_api = "filmes-API.json"
    camada = "Raw"
    api = "TMDB"

    data = datetime.now()

    dia = data.day
    mes = data.month
    ano = data.year

    df = pd.read_csv('movies.csv', sep='|', encoding='utf8', dtype={'anoLancamento': str})

    df = df[df['genero'].str.contains('War|Crime')]

    newdf = df.replace('\\N', 'Null')

    tamanho = 100

    arquivos = [newdf[i:i + tamanho] for i in range(0, len(newdf), tamanho)]

    upload_filme = f'{s3nome}/{camada}/{origem}/{formato}/{f}/{ano}/{mes}/{dia}/{filmes}'

    for i, parte in enumerate(arquivos):
        arquivo = f'filmes{i}.json'
        parte.to_json(arquivo, orient='records', lines=True, indent=4)
        s3.put_object(Bucket = s3nome, Key = upload_filme, Body = '\n'.join(arquivo))
