# dagster_university

This is a [Dagster](https://dagster.io/) project made to accompany Dagster University coursework.

## Getting started

First, install your Dagster code location as a Python package by running the command below in your terminal. By using the --editable (`-e`) flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Duplicate the `.env.example` file and rename it to `.env`.

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

---

## Tests

### Vue d'ensemble

Les tests sont écrits avec **pytest** et couvrent l'ensemble des assets Dagster du pipeline. Ils utilisent des **mocks** (faux objets) pour simuler les appels à l'API TMDB et à la base de données PostgreSQL, ce qui permet d'exécuter les tests sans connexion réelle ni clé API.

Le fichier de tests se trouve dans :
```
dagster_university_tests/test_assets.py
```

---

### Lancer les tests

Depuis le répertoire `dagster_university/` :

```bash
# Lancer tous les tests
pytest dagster_university_tests/

# Avec le détail de chaque test
pytest dagster_university_tests/ -v

# Un seul test par nom
pytest dagster_university_tests/ -k "test_get_movie_file_from_api_creates_file"
```

Résultat attendu : **19 tests passent**, aucun appel réseau ni base de données réelle n'est effectué.

---

### Tests par asset

#### `get_movie_file_from_api`

| Test | Ce qui est vérifié |
|---|---|
| `test_get_movie_file_from_api_creates_file` | Le fichier JSON est créé avec le bon nom (`movies_2025-03-01_2025-03-31.json`) |
| `test_get_movie_file_from_api_file_content` | Le fichier contient bien les données retournées par l'API |
| `test_get_movie_file_from_api_raises_on_error` | Une exception est levée si l'API retourne une erreur HTTP 500 |
| `test_get_movie_file_from_api_paginates` | Toutes les pages sont récupérées quand `total_pages > 1` |

#### `load_movie_into_db`

| Test | Ce qui est vérifié |
|---|---|
| `test_load_movie_into_db_executes_create_and_insert` | `CREATE TABLE` et `INSERT INTO movie` sont bien exécutés |
| `test_load_movie_into_db_deletes_before_insert` | `DELETE FROM movie` est exécuté avant l'insert (idempotence de la partition) |
| `test_load_movie_into_db_raises_if_file_missing` | `FileNotFoundError` est levée si le JSON de la partition est absent |
| `test_load_movie_into_db_handles_empty_release_date` | Une `release_date` vide est convertie en `None` (valeur SQL NULL) |

#### `get_genres_from_api`

| Test | Ce qui est vérifié |
|---|---|
| `test_get_genres_from_api_creates_table_and_inserts` | `CREATE TABLE genres` et `INSERT INTO genres` sont exécutés |
| `test_get_genres_from_api_skips_insert_on_empty` | Aucun `INSERT` n'est tenté si l'API retourne une liste vide |

#### `add_movie_revenues`

| Test | Ce qui est vérifié |
|---|---|
| `test_add_movie_revenues_calls_update` | `UPDATE movie SET revenue` est exécuté avec la valeur retournée par l'API |
| `test_add_movie_revenues_skips_on_error` | Aucun `UPDATE` n'est exécuté si l'API retourne une erreur 404 |
| `test_add_movie_revenues_sleeps_on_rate_limit` | `time.sleep(1)` est appelé quand l'API retourne 429 (rate limit) |

#### `create_movies_cleaned`

| Test | Ce qui est vérifié |
|---|---|
| `test_create_movies_cleaned_filters_revenue` | La requête contient le filtre `revenue > 0` |
| `test_create_movies_cleaned_filters_null_date` | La requête contient `release_date IS NOT NULL` |
| `test_create_movies_cleaned_expands_genre_ids` | La requête utilise `unnest(genre_ids)` pour dénormaliser les genres |

#### `transform_movies_for_analysis`

| Test | Ce qui est vérifié |
|---|---|
| `test_transform_movies_for_analysis_filters_8_years` | La requête filtre bien sur les 8 dernières années |

#### `create_genre_year_statistics`

| Test | Ce qui est vérifié |
|---|---|
| `test_create_genre_year_statistics_creates_csv` | Le fichier CSV de résultats est bien créé dans `data/outputs/` |
| `test_create_genre_year_statistics_csv_content` | Le CSV contient les colonnes attendues (`genre_name`, `avg_popularity`, `avg_revenue`, `popularity_revenue_correlation`) |
