import requests
import os
from . import constants
from dotenv import load_dotenv
from dagster import asset
import json

load_dotenv() 
api_token = os.getenv("API_TOKEN")


@asset
def get_movie_file_from_api ():

    first_day_to_fetch = '2025-03-03'
    last_day_to_fetch = '2026-03-04'

    all_movies = []
    current_page = 1
    total_pages = 1

    url = "https://api.themoviedb.org/3/discover/movie"

    headers = {"accept": "application/json",
                "Authorization": f"Bearer {api_token}"}

    while current_page <= total_pages:
        
        body = {
            "release_date.gte" : f"{first_day_to_fetch}",
            "release_date.lte" : f"{last_day_to_fetch}",
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

    with open(constants.MOVIES_TEMPLATE_FILE_PATH.format(first_day_to_fetch), "w") as output_file:
        json.dump(all_movies, output_file, ensure_ascii=False) #pour les films étrangés

get_movie_file_from_api ()

