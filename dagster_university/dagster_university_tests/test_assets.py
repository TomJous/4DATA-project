import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from dagster_university.assets.movies import get_movie_file_from_api, load_movie_into_db, add_movie_revenues
from dagster_university.assets import constants


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


@pytest.fixture
def mock_tmdb_response():
    """Simule une réponse paginée de l'API TMDB."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "results": MOCK_MOVIES,
        "total_pages": 1,
    }
    return mock_resp


def test_get_movie_file_from_api_creates_file(mock_tmdb_response, tmp_path, monkeypatch):
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

    expected_file = tmp_path / "movies_2025-03-01_2025-03-31.json"
    assert expected_file.exists(), "Le fichier JSON n'a pas été créé"


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

    expected_file = tmp_path / "movies_2025-03-01_2025-03-31.json"
    data = json.loads(expected_file.read_text(encoding="utf-8"))

    assert isinstance(data, list)
    assert len(data) == len(MOCK_MOVIES)
    assert data[0]["id"] == MOCK_MOVIES[0]["id"]
    assert data[0]["title"] == MOCK_MOVIES[0]["title"]


@pytest.fixture
def movie_json_file(tmp_path):
    """Crée un fichier JSON de films pour la partition 2025-03."""
    file_path = tmp_path / "movies_2025-03-01_2025-03-31.json"
    file_path.write_text(json.dumps(MOCK_MOVIES), encoding="utf-8")
    return tmp_path


def test_load_movie_into_db_creates_table(movie_json_file, monkeypatch):
    """Vérifie que la table movie est créée dans DuckDB avec les bonnes lignes."""
    db_path = str(movie_json_file / "test.duckdb")
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(movie_json_file / "movies_{}_{}.json"))

    database = DuckDBResource(database=db_path)
    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, database)

    with duckdb.connect(db_path) as conn:
        rows = conn.execute("SELECT id, title FROM movie").fetchall()

    assert len(rows) == len(MOCK_MOVIES)
    assert rows[0][0] == MOCK_MOVIES[0]["id"]
    assert rows[0][1] == MOCK_MOVIES[0]["title"]


def test_load_movie_into_db_idempotent(movie_json_file, monkeypatch):
    """Vérifie qu'exécuter l'asset deux fois ne duplique pas les données."""
    db_path = str(movie_json_file / "test.duckdb")
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(movie_json_file / "movies_{}_{}.json"))

    database = DuckDBResource(database=db_path)
    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, database)
    load_movie_into_db(context, database)

    with duckdb.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM movie").fetchone()[0]

    assert count == len(MOCK_MOVIES)


@pytest.fixture
def db_with_movies(tmp_path, monkeypatch):
    """Crée une DuckDB avec la table movie déjà chargée."""
    monkeypatch.setattr(constants, "MOVIES_TEMPLATE_FILE_PATH", str(tmp_path / "movies_{}_{}.json"))
    json_file = tmp_path / "movies_2025-03-01_2025-03-31.json"
    json_file.write_text(json.dumps(MOCK_MOVIES), encoding="utf-8")

    db_path = str(tmp_path / "test.duckdb")
    database = DuckDBResource(database=db_path)
    context = build_asset_context(partition_key=PARTITION_KEY)
    load_movie_into_db(context, database)
    return db_path


def test_add_movie_revenues_fills_column(db_with_movies):
    """Vérifie que la colonne revenue est remplie après l'asset."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"revenue": 1000000}

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        database = DuckDBResource(database=db_with_movies)
        context = build_asset_context(partition_key=PARTITION_KEY)
        add_movie_revenues(context, database)

    with duckdb.connect(db_with_movies) as conn:
        rows = conn.execute("SELECT revenue FROM movie").fetchall()

    assert all(row[0] == 1000000 for row in rows)


def test_add_movie_revenues_skips_on_error(db_with_movies):
    """Vérifie que l'asset ne plante pas si l'API retourne une erreur."""
    mock_resp = MagicMock()
    mock_resp.status_code = 404

    with patch("dagster_university.assets.movies.requests.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        database = DuckDBResource(database=db_with_movies)
        context = build_asset_context(partition_key=PARTITION_KEY)
        add_movie_revenues(context, database)

    with duckdb.connect(db_with_movies) as conn:
        rows = conn.execute("SELECT revenue FROM movie").fetchall()

    assert all(row[0] is None for row in rows)
