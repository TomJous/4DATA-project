import json
import os
import time
from datetime import datetime
from pathlib import Path
import calendar

from dotenv import load_dotenv
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
import requests

from . import constants
from ..partitions import monthly_partition


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
def get_genres_from_api(database: DuckDBResource) -> None:
    """Fetch TMDB movie genres and write them into DuckDB."""
    url = "https://api.themoviedb.org/3/genre/movie/list"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    genres = response.json().get("genres", [])
    query = """
        CREATE OR REPLACE TABLE genres(
            genre_id INTEGER,
            genre_name VARCHAR
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
        if genres:
            conn.executemany(
                "INSERT INTO genres VALUES(?, ?);",
                [(genre["id"], genre["name"]) for genre in genres],
            )

@asset(
    deps=["get_movie_file_from_api"],
    partitions_def=monthly_partition
)
def load_movie_into_db(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """Load the partition JSON file into the DuckDB movie table without dropping the entire table."""
    partition_date = context.partition_key
    start_date = partition_date

    date_obj = datetime.strptime(partition_date, "%Y-%m-%d")
    last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
    end_date = f"{date_obj.year}-{date_obj.month:02d}-{last_day}"

    file_path = Path(constants.MOVIES_TEMPLATE_FILE_PATH.format(start_date, end_date))
    if not file_path.exists():
        raise FileNotFoundError(f"Movie partition file not found: {file_path}")

    query_create = """
        CREATE TABLE IF NOT EXISTS movie (
            id INTEGER,
            title VARCHAR,
            original_title VARCHAR,
            original_language VARCHAR,
            overview VARCHAR,
            release_date DATE,
            genre_ids INTEGER[],
            popularity DOUBLE,
            vote_average DOUBLE,
            vote_count INTEGER,
            adult BOOLEAN,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            video BOOLEAN,
            partition_month DATE
        );
    """
    query_delete = f"DELETE FROM movie WHERE partition_month = DATE '{start_date}';"
    query_insert = f"""
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
    SELECT
        id,
        title,
        original_title,
        original_language,
        overview,
        TRY_CAST(NULLIF(release_date, '') AS DATE) AS release_date,
        genre_ids,
        popularity,
        vote_average,
        vote_count,
        adult,
        backdrop_path,
        poster_path,
        video,
        DATE '{start_date}' AS partition_month
    FROM '{file_path.as_posix()}';
    """

    with database.get_connection() as conn:
        conn.execute(query_create)
        conn.execute(query_delete)
        conn.execute(query_insert)

    context.log.info(
        "Data loaded into DuckDB for %s -> %s from %s",
        start_date,
        end_date,
        file_path,
    )


@asset(
    partitions_def=monthly_partition,
    deps=["load_movie_into_db"]
)
def add_movie_revenues(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """Enrich movie rows with revenue values from the TMDB movie details endpoint."""
    with requests.Session() as session:
        headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}

        with database.get_connection() as conn:
            conn.execute("ALTER TABLE movie ADD COLUMN IF NOT EXISTS revenue BIGINT;")
            movie_rows = conn.execute(
                """
                SELECT id
                FROM movie
                WHERE revenue IS NULL
                """
            ).fetchall()

            for (movie_id,) in movie_rows:
                url = f"https://api.themoviedb.org/3/movie/{movie_id}"
                response = session.get(url, headers=headers)

                if response.status_code == 200:
                    revenue = response.json().get("revenue")
                    conn.execute(
                        "UPDATE movie SET revenue = ? WHERE id = ?;",
                        [revenue, movie_id],
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

