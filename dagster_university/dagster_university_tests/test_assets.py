import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import requests as requests_lib

import pandas as pd

import pytest
from dagster import build_asset_context
from sqlalchemy import text

from dagster_university.assets.movies import (
    add_movie_revenues,
    get_genres_from_api,
    get_movie_file_from_api,
    load_movie_into_db,
)
from dagster_university.assets.metrics import (
    create_movies_cleaned,
    transform_movies_for_analysis,
    create_genre_year_statistics,
)
from dagster_university.assets import constants
from dagster_university.resources import PostgresResource


PARTITION_KEY = "2025-03-01"

MOCK_MOVIES = [
    {
        "id": 1,
        "title": "Test Movie",
        "original_title": "Test Movie",
        "original_language": "en",
        "overview": "A test movie.",
        "release_date": "2025-03-15",
        "genre_ids": [28, 12],
        "popularity": 10.5,
        "vote_average": 7.0,
        "vote_count": 100,
        "adult": False,
        "backdrop_path": "/backdrop.jpg",
        "poster_path": "/poster.jpg",
        "video": False,
    }
]


# ---------------------------------------------------------------------------
# Fixtures partagées
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_tmdb_response():
    """Simule une réponse paginée de l'API TMDB."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"results": MOCK_MOVIES, "total_pages": 1}
    return mock_resp


@pytest.fixture
def mock_postgres():
    """Crée un vrai PostgresResource mais avec une connexion mockée."""
    mock_conn = MagicMock() #faux objet de connexion

    @contextmanager
    def fake_get_connection(self): 
        yield mock_conn

    with patch.object(PostgresResource, "get_connection", fake_get_connection): #remplace la lecture de la connexion avec postgres par notre mock
        resource = PostgresResource(connection_string="postgresql://fake:fake@localhost/fake")
        yield resource, mock_conn


@pytest.fixture
def movie_json_file(tmp_path, monkeypatch):
    """Crée un fichier JSON de films pour la partition 2025-03."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))
    file_path = tmp_path / "movies_2025-03-01_2025-03-31.json"
    file_path.write_text(json.dumps(MOCK_MOVIES), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Tests get_movie_file_from_api
# ---------------------------------------------------------------------------

def test_get_movie_file_from_api_creates_file(mock_tmdb_response, tmp_path, monkeypatch): #tmp_path = crée un dossier temporaire, monkeypatch sert à modifier temporairement des valeurs
    """Vérifie que l'asset crée bien le fichier JSON avec le bon nom."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_tmdb_response
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        get_movie_file_from_api(context)

    assert (tmp_path / "movies_2025-03-01_2025-03-31.json").exists()


def test_get_movie_file_from_api_file_content(mock_tmdb_response, tmp_path, monkeypatch):
    """Vérifie que le fichier JSON contient les données retournées par l'API."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_tmdb_response
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        get_movie_file_from_api(context)

    data = json.loads((tmp_path / "movies_2025-03-01_2025-03-31.json").read_text())
    assert isinstance(data, list)
    assert len(data) == len(MOCK_MOVIES)
    assert data[0]["id"] == MOCK_MOVIES[0]["id"]
    assert data[0]["title"] == MOCK_MOVIES[0]["title"]



# ---------------------------------------------------------------------------
# Tests load_movie_into_db
# ---------------------------------------------------------------------------

def test_load_movie_into_db_executes_create_and_insert(movie_json_file, mock_postgres):
    """Vérifie que CREATE TABLE et INSERT sont bien exécutés."""
    resource, mock_conn = mock_postgres

    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS movie" in q for q in executed_queries)
    assert any("INSERT INTO movie" in q for q in executed_queries)


def test_load_movie_into_db_deletes_before_insert(movie_json_file, mock_postgres):
    """Vérifie que DELETE est exécuté avant INSERT pour garantir l'idempotence."""
    resource, mock_conn = mock_postgres

    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("DELETE FROM movie" in q for q in executed_queries)


# ---------------------------------------------------------------------------
# Tests add_movie_revenues
# ---------------------------------------------------------------------------

def test_add_movie_revenues_calls_update(mock_postgres):
    """Vérifie que UPDATE est appelé avec le bon revenue pour chaque film."""
    resource, mock_conn = mock_postgres
    mock_conn.execute.return_value.fetchall.return_value = [(1,)]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"revenue": 1000000}

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        add_movie_revenues(context, resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("UPDATE movie AS m SET revenue" in q for q in executed_queries)


def test_add_movie_revenues_skips_on_error(mock_postgres):
    """Vérifie que l'asset ne plante pas si l'API retourne une erreur 404."""
    resource, mock_conn = mock_postgres
    mock_conn.execute.return_value.fetchall.return_value = [(1,)]

    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.raise_for_status.side_effect = requests_lib.HTTPError(response=mock_resp)

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        add_movie_revenues(context, resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert not any("UPDATE movie AS m SET revenue" in q for q in executed_queries)


# ---------------------------------------------------------------------------
# Tests get_movie_file_from_api — robustesse
# ---------------------------------------------------------------------------

def test_get_movie_file_from_api_raises_on_error(tmp_path, monkeypatch):
    """Vérifie que l'asset lève une exception si l'API retourne une erreur."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.raise_for_status.side_effect = Exception("API error")

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        with pytest.raises(Exception, match="API error"):
            get_movie_file_from_api(context)


def test_get_movie_file_from_api_paginates(tmp_path, monkeypatch):
    """Vérifie que l'asset récupère bien toutes les pages."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))

    page1 = MagicMock()
    page1.status_code = 200
    page1.json.return_value = {"results": [MOCK_MOVIES[0]], "total_pages": 2}

    page2 = MagicMock()
    page2.status_code = 200
    page2.json.return_value = {"results": [MOCK_MOVIES[0]], "total_pages": 2}

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.side_effect = [page1, page2]
        mock_session_cls.return_value = mock_session

        context = build_asset_context(partition_key=PARTITION_KEY)
        get_movie_file_from_api(context)

    data = json.loads((tmp_path / "movies_2025-03-01_2025-03-31.json").read_text())
    assert len(data) == 2


# ---------------------------------------------------------------------------
# Tests load_movie_into_db — robustesse
# ---------------------------------------------------------------------------

def test_load_movie_into_db_raises_if_file_missing(mock_postgres, monkeypatch, tmp_path):
    """Vérifie que l'asset lève FileNotFoundError si le JSON est absent."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))
    resource, _ = mock_postgres

    context = build_asset_context(partition_key=PARTITION_KEY)
    with pytest.raises(FileNotFoundError):
        load_movie_into_db(context, resource)


def test_load_movie_into_db_handles_empty_release_date(mock_postgres, tmp_path, monkeypatch):
    """Vérifie que release_date vide devient None sans planter."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))
    movie_with_empty_date = {**MOCK_MOVIES[0], "release_date": ""} #** crée une copie du MOCK_MOVIES
    (tmp_path / "movies_2025-03-01_2025-03-31.json").write_text(
        json.dumps([movie_with_empty_date]), encoding="utf-8"
    )

    resource, mock_conn = mock_postgres
    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, resource)

    insert_call = next(
        c for c in mock_conn.execute.call_args_list
        if "INSERT INTO movie" in str(c.args[0]) # args[0] = la requête SQL, args[1] = les rows
    )
    rows = insert_call.args[1] 
    assert rows[0]["release_date"] is None


# ---------------------------------------------------------------------------
# Tests create_movies_cleaned
# ---------------------------------------------------------------------------

def test_create_movies_cleaned_filters_revenue(mock_postgres):
    """Vérifie que le filtre revenue > 0 est bien dans la requête."""
    resource, mock_conn = mock_postgres
    create_movies_cleaned(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("revenue > 0" in q for q in executed_queries)


def test_create_movies_cleaned_filters_null_date(mock_postgres):
    """Vérifie que le filtre release_date IS NOT NULL est bien dans la requête."""
    resource, mock_conn = mock_postgres
    create_movies_cleaned(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("release_date IS NOT NULL" in q for q in executed_queries)


def test_create_movies_cleaned_expands_genre_ids(mock_postgres):
    """Vérifie que unnest(genre_ids) est bien dans la requête."""
    resource, mock_conn = mock_postgres
    create_movies_cleaned(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("unnest(genre_ids)" in q for q in executed_queries)


# ---------------------------------------------------------------------------
# Tests transform_movies_for_analysis
# ---------------------------------------------------------------------------

def test_transform_movies_for_analysis_filters_8_years(mock_postgres):
    """Vérifie que le filtre 8 ans est bien présent"""
    resource, mock_conn = mock_postgres
    transform_movies_for_analysis(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("- 8" in q for q in executed_queries)


# ---------------------------------------------------------------------------
# Tests get_genres_from_api
# ---------------------------------------------------------------------------

def test_get_genres_from_api_creates_table_and_inserts(mock_postgres):
    """Vérifie que CREATE TABLE genres et INSERT sont exécutés."""
    resource, mock_conn = mock_postgres

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"genres": [{"id": 28, "name": "Action"}]}

    with patch("dagster_university.assets.movies.requests.get", return_value=mock_resp):
        get_genres_from_api(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any("CREATE TABLE genres" in q for q in executed_queries)
    assert any("INSERT INTO genres" in q for q in executed_queries)


def test_get_genres_from_api_skips_insert_on_empty(mock_postgres):
    """Vérifie qu'aucun INSERT n'est tenté si l'API retourne une liste vide."""
    resource, mock_conn = mock_postgres

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"genres": []}

    with patch("dagster_university.assets.movies.requests.get", return_value=mock_resp):
        get_genres_from_api(resource)

    executed_queries = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert not any("INSERT INTO genres" in q for q in executed_queries)


# ---------------------------------------------------------------------------
# Tests create_genre_year_statistics
# ---------------------------------------------------------------------------

def test_create_genre_year_statistics_creates_csv(mock_postgres, tmp_path, monkeypatch):
    """Vérifie que le fichier CSV est bien créé dans data/outputs/."""
    csv_path = tmp_path / "outputs" / "genre_year_statistics.csv"
    monkeypatch.setattr(constants, "GENRE_YEAR_STATISTICS", str(csv_path))

    fake_df = pd.DataFrame([
        {"genre_name": "Action", "release_year": 2024, "avg_popularity": 15.0,
         "avg_revenue": 500000, "nb_films": 10, "popularity_revenue_correlation": 0.8}
    ])

    resource, mock_conn = mock_postgres

    with patch("dagster_university.assets.metrics.pd.read_sql", return_value=fake_df):
        create_genre_year_statistics(resource)

    assert csv_path.exists(), "Le fichier CSV n'a pas été créé"


def test_create_genre_year_statistics_csv_content(mock_postgres, tmp_path, monkeypatch):
    """Vérifie que le CSV contient les bonnes colonnes."""
    csv_path = tmp_path / "outputs" / "genre_year_statistics.csv"
    monkeypatch.setattr(constants, "GENRE_YEAR_STATISTICS", str(csv_path))

    fake_df = pd.DataFrame([
        {"genre_name": "Action", "release_year": 2024, "avg_popularity": 15.0,
         "avg_revenue": 500000, "nb_films": 10, "popularity_revenue_correlation": 0.8}
    ])

    resource, mock_conn = mock_postgres

    with patch("dagster_university.assets.metrics.pd.read_sql", return_value=fake_df):
        create_genre_year_statistics(resource)

    result = pd.read_csv(csv_path)
    assert "genre_name" in result.columns
    assert "avg_popularity" in result.columns
    assert "avg_revenue" in result.columns
    assert "popularity_revenue_correlation" in result.columns
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Tests add_movie_revenues — limit
# ---------------------------------------------------------------------------

def test_add_movie_revenues_sleeps_on_rate_limit(mock_postgres):
    """Vérifie que time.sleep est appelé quand l'API retourne 429."""
    resource, mock_conn = mock_postgres
    mock_conn.execute.return_value.fetchall.return_value = [(1,)]

    mock_resp = MagicMock()
    mock_resp.status_code = 429

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        with patch("dagster_university.assets.movies.time.sleep") as mock_sleep:
            context = build_asset_context(partition_key=PARTITION_KEY)
            add_movie_revenues(context, resource)

    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(1)
