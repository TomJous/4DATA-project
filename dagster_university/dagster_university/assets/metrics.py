from dagster import asset
from dagster_duckdb import DuckDBResource

from . import constants




@asset(
    deps=["add_movie_revenues"]
)
def create_movies_cleaned(database: DuckDBResource) -> None:
    
    query="""
            CREATE OR REPLACE TABLE movies_cleaned AS
            SELECT
                id,
                title,
                CAST(strftime('%Y', CAST(release_date AS DATE)) AS INTEGER) AS release_year,
                popularity,
                revenue,
                vote_average,
                vote_count,
                UNNEST(genre_ids) AS genre_id
            FROM movie
            WHERE release_date IS NOT NULL
              AND revenue IS NOT NULL
              AND revenue > 0
              AND popularity IS NOT NULL;
        """
    with database.get_connection() as conn:
        conn.execute(query)

@asset(
    deps=["create_movies_cleaned","get_genres_from_api"]
)
def transform_movies_for_analysis(database:DuckDBResource)->None:
    query="""
    CREATE OR REPLACE TABLE movies_analysis AS
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
        conn.execute(query)
    

@asset(
    deps=["transform_movies_for_analysis"]
)
def create_genre_year_statistics(database: DuckDBResource) -> None:

    query="""
            CREATE OR REPLACE TABLE genre_year_statistics AS
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
        conn.execute(query)
    
    with database.get_connection() as conn:
       genre_year_statistics= conn.execute("""
        SELECT *
        FROM genre_year_statistics;
        """).fetch_df()
    genre_year_statistics.to_csv(constants.GENRE_YEAR_STATISTICS,index=False)