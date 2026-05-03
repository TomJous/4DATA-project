import os

from dagster import asset
import pandas as pd
from sqlalchemy import text

from . import constants
from ..resources import PostgresResource



@asset(
    deps=["add_movie_revenues"]
)
def create_movies_cleaned(database: PostgresResource) -> None:
    """Build a cleaned movies table and expand genre IDs into rows."""
    query = """
    DROP TABLE IF EXISTS movies_cleaned;
    CREATE TABLE movies_cleaned AS
    SELECT
        id,
        title,
        EXTRACT(YEAR FROM release_date)::INTEGER AS release_year,
        popularity,
        revenue,
        vote_average,
        vote_count,
        genre_id
    FROM movie
    CROSS JOIN LATERAL unnest(genre_ids) AS genre_id
    WHERE release_date IS NOT NULL
      AND revenue IS NOT NULL
      AND revenue > 0
      AND popularity IS NOT NULL;
    """

    with database.get_connection() as conn:
        conn.execute(text(query))


@asset(
    deps=["create_movies_cleaned", "get_genres_from_api"]
)
def transform_movies_for_analysis(database: PostgresResource) -> None:
    """Create a denormalized analysis table combining movies and genres."""
    query = """
    DROP TABLE IF EXISTS movies_analysis;
    CREATE TABLE movies_analysis AS
    SELECT
        m.id,
        m.title,
        m.release_year,
        g.genre_name,
        m.popularity,
        m.revenue,
        m.vote_average,
        m.vote_count
    FROM movies_cleaned m
    JOIN genres g
        ON m.genre_id = g.genre_id
    WHERE m.release_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 8;
    """

    with database.get_connection() as conn:
        conn.execute(text(query))


@asset(
    deps=["transform_movies_for_analysis"]
)
def create_movie_title_analysis(database: PostgresResource) -> None:
    """Create one analysis row per movie/title and export it as CSV."""

    query = """
    DROP TABLE IF EXISTS movie_title_analysis;
    CREATE TABLE movie_title_analysis AS
    SELECT
        id,
        title,
        release_year,
        STRING_AGG(DISTINCT genre_name, ', ' ORDER BY genre_name) AS genres,
        MAX(popularity) AS popularity,
        MAX(revenue) AS revenue,
        MAX(vote_average) AS vote_average,
        MAX(vote_count) AS vote_count,
        CASE
            WHEN MAX(popularity) > 0 THEN MAX(revenue) / MAX(popularity)
            ELSE NULL
        END AS revenue_per_popularity_point
    FROM movies_analysis
    GROUP BY id, title, release_year
    ORDER BY revenue DESC, popularity DESC, title;
    """

    with database.get_connection() as conn:
        conn.execute(text(query))
        movie_title_analysis = pd.read_sql(
            text("""
            SELECT *
            FROM movie_title_analysis;
            """),
            conn,
        )

    os.makedirs(os.path.dirname(constants.MOVIE_TITLE_ANALYSIS), exist_ok=True)
    movie_title_analysis.to_csv(constants.MOVIE_TITLE_ANALYSIS, index=False, encoding="utf-8")


@asset(
    deps=["transform_movies_for_analysis"]
)
def create_genre_year_statistics(database: PostgresResource) -> None:
    """Aggregate genre/year metrics and export the result as CSV."""

    query = """
    DROP TABLE IF EXISTS genre_year_statistics;
    CREATE TABLE genre_year_statistics AS
    SELECT
        genre_name,
        release_year,
        AVG(popularity) AS avg_popularity,
        AVG(revenue) AS avg_revenue,
        COUNT(DISTINCT id) AS nb_films,
        corr(popularity, revenue) AS popularity_revenue_correlation
    FROM movies_analysis
    GROUP BY genre_name, release_year
    ORDER BY release_year, genre_name;
    """

    with database.get_connection() as conn:
        conn.execute(text(query))
        genre_year_statistics = pd.read_sql(
            text("""
            SELECT *
            FROM genre_year_statistics;
            """),
            conn,
        )

    os.makedirs(os.path.dirname(constants.GENRE_YEAR_STATISTICS), exist_ok=True)
    genre_year_statistics.to_csv(constants.GENRE_YEAR_STATISTICS, index=False, encoding="utf-8")
