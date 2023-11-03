import json
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):

    origem = "s3"
    s3nome = "desafio0"
    formato = "JSON"
    f = "Movies"
    filmes_api = "filmes-API.json"
    camada = "Raw"
    api = "TMDB"
    api_key = "bbb8dac32d651ea9d3da4edd35c1883b"

    def filmes(page):
        url = f"https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&language=pt-BR&page={page}&with_genres=10752"
        movies = requests.get(url)
        return movies.json()['results']

    data = datetime.now()
    dia, mes, ano = data.day, data.month, data.year

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(filmes, page) for page in range(1, 381)]

    results = [future.result() for future in futures]

    df = pd.DataFrame()
    for result in results:
        temporary_df = pd.DataFrame(result)
        temporary_df = temporary_df.rename(columns={'adult': '+18',
                                          'genre_ids': 'Genero',
                                          'original_language': 'Lingua original',
                                          'original_title': 'Titulo orignial',
                                          'overview': 'Overview',
                                          'popularity': 'Popularidade',
                                          'release_date': 'Data de lancamento',
                                          'title': 'Titulo',
                                          'video': 'Trailer',
                                          'vote_average': 'Media de votos',
                                          'vote_count': 'Contagem dos votos'})
    df = pd.concat([df, temporary_df], ignore_index=True)

    def duracao(movie_id):
        details_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=pt-BR&with_genres=10752"
        details_response = requests.get(details_url)
        if details_response.status_code == 200:
            details_data = details_response.json()
            return details_data.get('runtime')
        else:
            return None

    df['Duracao'] = df['id'].apply(duracao)

    upload_api = f'{s3nome}/{camada}/{api}/{formato}/{f}/{ano}/{mes}/{dia}/{filmes_api}'

    tamanho = 100

    arquivos = [df[i:i + tamanho] for i in range(0, len(df), tamanho)]

    for i, parte in enumerate(arquivos):
        arquivo = f'filmes-api{i}.json'
        s3.put_object(Bucket=s3nome, Key=f'{upload_api}/{arquivo}', Body=parte.to_json(orient='records', lines=True, indent=4))
