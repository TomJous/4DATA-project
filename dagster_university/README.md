# Analyse de l'influence de la popularité sur les revenus des films

## Présentation du projet

> **Comment l'indice de popularité influence-t-il le revenu des films en fonction du genre cinématographique sur les 8 dernières années ?**

Ce projet est un pipeline de données complet, de la collecte brute jusqu'à la visualisation, construit autour de l'API [TheMovieDB (TMDB)](https://developer.themoviedb.org) et orchestré avec [Dagster](https://dagster.io/).

### Stack technique

| Composant | Technologie |
|---|---|
| Orchestration | Dagster |
| Base de données | PostgreSQL |
| Visualisation | Streamlit |
| Source de données | API TheMovieDB |
| Conteneurisation | Docker / Docker Compose |

---

## Pipeline de données

### Assets

Le pipeline est découpé en assets exécutés dans cet ordre :

| Asset | Rôle |
|---|---|
| `get_movie_file_from_api` | Télécharge les films du mois depuis l'API TMDB et les sauvegarde en JSON |
| `get_genres_from_api` | Récupère la liste des genres cinématographiques et les insère en base |
| `load_movie_into_db` | Charge le fichier JSON de la partition mensuelle dans PostgreSQL |
| `add_movie_revenues` | Enrichit chaque film avec son revenu via l'endpoint TMDB dédié |
| `create_movies_cleaned` | Filtre les films sans revenu ni date, dénormalise les genres |
| `transform_movies_for_analysis` | Joint avec les genres, filtre sur les 8 dernières années |
| `create_genre_year_statistics` | Calcule les statistiques par genre/année et exporte un CSV |

### Jobs et schedules

| Nom | Déclenchement | Périmètre |
|---|---|---|
| `movie_monthly_job` | Manuel ou automatique | Tous les assets, partitionné par mois |
| Schedule mensuel | 1er de chaque mois | Exécute `movie_monthly_job` sur la partition du mois courant |

### Partitions

Les assets liés à la collecte et au chargement sont **partitionnés par mois** (de janvier 2017 à aujourd'hui). Cela permet de relancer uniquement une période précise sans retraiter toute l'histoire.

---

## Lancer le projet

### Prérequis

- Docker et Docker Compose installés
- Une clé API TheMovieDB (gratuite sur [themoviedb.org](https://developer.themoviedb.org))

### Démarrage

```bash
cp .env.example .env   # remplir API_TOKEN et POSTGRES_PASSWORD
docker compose up --build
```

- Interface Dagster : http://localhost:3000
- Tableau de bord Streamlit : http://localhost:8501

### Lancer les tests en local

Les tests ne nécessitent pas Docker ni de clé API. Il faut installer les dépendances une seule fois :

```bash
pip install -e ".[dev]"
```

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
