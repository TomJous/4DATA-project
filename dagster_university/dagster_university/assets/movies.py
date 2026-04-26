import requests
import os
import time
from . import constants
from dotenv import load_dotenv
from dagster import asset, Config
import json

from dagster_duckdb import DuckDBResource
from .requests import MovieConfig


load_dotenv() 
api_token = os.getenv("API_TOKEN")

# faire en sorte de réupérer qu'un partitionnement des donnés 

# gérer le docker 

@asset
def get_movie_file_from_api(config: MovieConfig):

    all_movies = []
    current_page = 1
    total_pages = 1

    url = "https://api.themoviedb.org/3/discover/movie"

    headers = {"accept": "application/json",
                "Authorization": f"Bearer {api_token}"}

    while current_page <= total_pages:

        body = {
            "release_date.gte" : config.start_date,
            "release_date.lte" : config.end_date,
            "page" : f"{current_page}"
        }

        raw_movies = requests.get(url, headers=headers, params=body)

        if raw_movies.status_code == 200:
            data = raw_movies.json()
            all_movies.extend(data.get("results", [])) # ajoute une liste vide si il n'y aucun résultat
            total_pages = data.get("total_pages", 1)
            current_page += 1
        else:
            print(f"Erreur à la page {current_page}: {raw_movies.text}")
            break

    file_path = constants.MOVIES_TEMPLATE_FILE_PATH.format(config.start_date, config.end_date)
    with open(file_path, "w") as output_file:
        json.dump(all_movies, output_file, ensure_ascii=False) #pour les films étrangés

@asset(
deps=["get_movie_file_from_api"]
)
def load_movie_into_db(config: MovieConfig, database: DuckDBResource) -> None:

    #par défault la requête pour découvrire les films de nous transmet pas les revenues générés
    file_path = constants.MOVIES_TEMPLATE_FILE_PATH.format(config.start_date, config.end_date)

    query = f"""
        CREATE OR REPLACE TABLE movie AS (
        SELECT
        id,
        title,
        original_title,
        original_language,
        overview,
        release_date,
        genre_ids,
        popularity,
        vote_average,
        vote_count,
        adult,
        backdrop_path,
        poster_path,
        video
        FROM '{file_path}'
        );
"""

    with database.get_connection() as conn:
        conn.execute(query)

@asset(
    deps = ["load_movie_into_db"]
)
def add_movie_revenues(database: DuckDBResource)-> None:
    with database.get_connection() as conn:
        query = "ALTER TABLE movie ADD COLUMN revenue BIGINT;"
        conn.execute(query)
        query = "SELECT id FROM movie;"
        movies = conn.execute(query).fetchall()
        for movie in movies:

            url = f"https://api.themoviedb.org/3/movie/{movie[0]}"
            headers = {"accept": "application/json",
                    "Authorization": f"Bearer {api_token}"}
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                revenue = response.json().get("revenue")
                inject_revenue = f"UPDATE movie SET revenue = {revenue} WHERE id = {movie[0]};"
                conn.execute(inject_revenue)
            else:
                time.sleep(1)

