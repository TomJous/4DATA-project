import json
import os
import time
from datetime import datetime
from pathlib import Path
import calendar

from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext
import requests
from sqlalchemy import text

from . import constants
from ..partitions import monthly_partition
from ..resources import PostgresResource


load_dotenv()
api_token = os.getenv("API_TOKEN")
if not api_token:
    raise RuntimeError("API_TOKEN is required for TMDB access. Set it in the environment or .env file.")

@asset(
    partitions_def=monthly_partition
)
def get_movie_file_from_api(context: AssetExecutionContext) -> None:
    """Download TMDB movies for the current monthly partition and save a JSON file."""
    partition_date = context.partition_key
    start_date = partition_date

    date_obj = datetime.strptime(partition_date, "%Y-%m-%d")
    last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
    end_date = f"{date_obj.year}-{date_obj.month:02d}-{last_day}"

    all_movies = []
    current_page = 1
    total_pages = 1
    url = "https://api.themoviedb.org/3/discover/movie"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}

    with requests.Session() as session:
        while current_page <= total_pages:
            params = {
                "release_date.gte": start_date,
                "release_date.lte": end_date,
                "page": current_page,
            }
            response = session.get(url, headers=headers, params=params)

            if response.status_code != 200:
                context.log.error(
                    "Failed to download TMDB page %s: %s",
                    current_page,
                    response.text,
                )
                response.raise_for_status()

            data = response.json()
            all_movies.extend(data.get("results", []))
            total_pages = data.get("total_pages", 1)
            current_page += 1

    file_path = Path(constants.MOVIES_TEMPLATE_FILE_PATH.format(start_date, end_date))
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(all_movies, ensure_ascii=False), encoding="utf-8")

    context.log.info(
        "%d movies retrieved for %s -> %s and written to %s",
        len(all_movies),
        start_date,
        end_date,
        file_path,
    )


@asset
def get_genres_from_api(database: PostgresResource) -> None:
    """Fetch TMDB movie genres and write them into Postgres."""
    url = "https://api.themoviedb.org/3/genre/movie/list"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    genres = response.json().get("genres", [])
    query = """
        DROP TABLE IF EXISTS genres;
        CREATE TABLE genres(
            genre_id INTEGER,
            genre_name VARCHAR
        );
    """

    with database.get_connection() as conn:
        conn.execute(text(query))
        if genres:
            conn.execute(
                text("INSERT INTO genres VALUES(:genre_id, :genre_name);"),
                [
                    {"genre_id": genre["id"], "genre_name": genre["name"]}
                    for genre in genres
                ],
            )

@asset(
    deps=["get_movie_file_from_api"],
    partitions_def=monthly_partition
)
def load_movie_into_db(context: AssetExecutionContext, database: PostgresResource) -> None:
    """Load the partition JSON file into the Postgres movie table."""
    partition_date = context.partition_key
    start_date = partition_date

    date_obj = datetime.strptime(partition_date, "%Y-%m-%d")
    last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
    end_date = f"{date_obj.year}-{date_obj.month:02d}-{last_day}"

    file_path = Path(constants.MOVIES_TEMPLATE_FILE_PATH.format(start_date, end_date))
    if not file_path.exists():
        raise FileNotFoundError(f"Movie partition file not found: {file_path}")

    movies = json.loads(file_path.read_text(encoding="utf-8"))
    rows = [
        {
            "id": movie.get("id"),
            "title": movie.get("title"),
            "original_title": movie.get("original_title"),
            "original_language": movie.get("original_language"),
            "overview": movie.get("overview"),
            "release_date": movie.get("release_date") or None,
            "genre_ids": movie.get("genre_ids") or [],
            "popularity": movie.get("popularity"),
            "vote_average": movie.get("vote_average"),
            "vote_count": movie.get("vote_count"),
            "adult": movie.get("adult"),
            "backdrop_path": movie.get("backdrop_path"),
            "poster_path": movie.get("poster_path"),
            "video": movie.get("video"),
            "partition_month": start_date,
        }
        for movie in movies
    ]

    query_create = """
        CREATE TABLE IF NOT EXISTS movie (
            id INTEGER,
            title VARCHAR,
            original_title VARCHAR,
            original_language VARCHAR,
            overview VARCHAR,
            release_date DATE,
            genre_ids INTEGER[],
            popularity DOUBLE PRECISION,
            vote_average DOUBLE PRECISION,
            vote_count INTEGER,
            adult BOOLEAN,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            video BOOLEAN,
            partition_month DATE
        );
    """
    query_insert = """
        INSERT INTO movie (
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
            video,
            partition_month
        )
        VALUES (
            :id,
            :title,
            :original_title,
            :original_language,
            :overview,
            :release_date,
            :genre_ids,
            :popularity,
            :vote_average,
            :vote_count,
            :adult,
            :backdrop_path,
            :poster_path,
            :video,
            :partition_month
        );
    """

    with database.get_connection() as conn:
        conn.execute(text(query_create))
        conn.execute(
            text("DELETE FROM movie WHERE partition_month = :partition_month;"),
            {"partition_month": start_date},
        )
        if rows:
            conn.execute(text(query_insert), rows)

    context.log.info(
        "Data loaded into Postgres for %s -> %s from %s",
        start_date,
        end_date,
        file_path,
    )


@asset(
    partitions_def=monthly_partition,
    deps=["load_movie_into_db"]
)
def add_movie_revenues(context: AssetExecutionContext, database: PostgresResource) -> None:
    """Enrich movie rows with revenue values from the TMDB movie details endpoint."""
    with requests.Session() as session:
        headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}

        with database.get_connection() as conn:
            conn.execute(text("ALTER TABLE movie ADD COLUMN IF NOT EXISTS revenue BIGINT;"))
            movie_rows = conn.execute(
                text("""
                SELECT id
                FROM movie
                WHERE revenue IS NULL
                """)
            ).fetchall()

            for (movie_id,) in movie_rows:
                url = f"https://api.themoviedb.org/3/movie/{movie_id}"
                response = session.get(url, headers=headers)

                if response.status_code == 200:
                    revenue = response.json().get("revenue")
                    conn.execute(
                        text("UPDATE movie SET revenue = :revenue WHERE id = :movie_id;"),
                        {"revenue": revenue, "movie_id": movie_id},
                    )
                elif response.status_code == 429:
                    context.log.warning(
                        "TMDB rate limit reached for movie %s, retrying after sleep.",
                        movie_id,
                    )
                    time.sleep(1)
                else:
                    context.log.warning(
                        "Failed to fetch revenue for movie %s: %s",
                        movie_id,
                        response.status_code,
                    )
                    time.sleep(1)

    context.log.info("Revenues added successfully.")
