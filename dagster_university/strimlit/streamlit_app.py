import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Analyse Films TMDB",
    page_icon="🎬",
    layout="wide"
)

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "movies")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def safe_correlation(df, left_col, right_col):
    if len(df) < 2 or df[left_col].nunique() < 2 or df[right_col].nunique() < 2:
        return None

    return df[left_col].corr(df[right_col])

@st.cache_data
def load_data():
    engine = create_engine(DATABASE_URL)

    title_query = """
        SELECT
            id,
            title,
            release_year,
            genres,
            popularity,
            revenue,
            vote_average,
            vote_count,
            revenue_per_popularity_point
        FROM movie_title_analysis
        WHERE revenue IS NOT NULL
          AND revenue > 0
          AND popularity IS NOT NULL;
    """

    genre_query = """
        SELECT
            id,
            title,
            release_year,
            genre_name,
            popularity,
            revenue,
            vote_average,
            vote_count
        FROM movies_analysis
        WHERE revenue IS NOT NULL
          AND revenue > 0
          AND popularity IS NOT NULL;
    """

    title_df = pd.read_sql(title_query, engine)
    genre_df = pd.read_sql(genre_query, engine)

    return title_df, genre_df


st.title("🎬 Analyse de l'influence de la popularité sur les revenus des films")

st.markdown("""
Ce tableau de bord permet d'analyser la relation entre **l'indice de popularité**
et le **revenu des films**, selon le **genre cinématographique**, sur les dernières années.
""")

try:
    title_df, genre_df = load_data()
except Exception as e:
    st.error("Impossible de charger les données depuis PostgreSQL.")
    st.exception(e)
    st.stop()

if title_df.empty:
    st.warning("Aucune donnée disponible.")
    st.stop()

st.sidebar.header("Filtres")

min_year = int(title_df["release_year"].min())
max_year = int(title_df["release_year"].max())

selected_years = st.sidebar.slider(
    "Période",
    min_value=min_year,
    max_value=max_year,
    value=(max(min_year, max_year - 7), max_year)
)

genres = sorted(genre_df["genre_name"].dropna().unique())

selected_genres = st.sidebar.multiselect(
    "Genres",
    options=genres,
    default=genres
)

selected_titles = st.sidebar.multiselect(
    "Titres",
    options=sorted(title_df["title"].dropna().unique()),
    default=[]
)

filtered_titles = title_df[
    (title_df["release_year"] >= selected_years[0]) &
    (title_df["release_year"] <= selected_years[1])
]

if selected_genres:
    filtered_titles = filtered_titles[
        filtered_titles["genres"]
        .fillna("")
        .apply(lambda movie_genres: any(genre in movie_genres.split(", ") for genre in selected_genres))
    ]

if selected_titles:
    filtered_titles = filtered_titles[filtered_titles["title"].isin(selected_titles)]

filtered_genres = genre_df[
    (genre_df["release_year"] >= selected_years[0]) &
    (genre_df["release_year"] <= selected_years[1])
]

if selected_genres:
    filtered_genres = filtered_genres[filtered_genres["genre_name"].isin(selected_genres)]

if selected_titles:
    filtered_genres = filtered_genres[filtered_genres["title"].isin(selected_titles)]

if filtered_titles.empty:
    st.warning("Aucune donnée ne correspond aux filtres sélectionnés.")
    st.stop()

st.subheader("📌 Indicateurs clés")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Nombre de films", f"{filtered_titles['id'].nunique():,}")
col2.metric("Revenu moyen", f"${filtered_titles['revenue'].mean():,.0f}")
col3.metric("Popularité moyenne", f"{filtered_titles['popularity'].mean():.2f}")
col4.metric("Note moyenne", f"{filtered_titles['vote_average'].mean():.2f}/10")

st.subheader("📈 Corrélation popularité / revenu")

correlation = safe_correlation(filtered_titles, "popularity", "revenue")

if correlation is None:
    st.info("Pas assez de données pour calculer une corrélation globale.")
else:
    st.info(f"Corrélation globale entre popularité et revenu : **{correlation:.2f}**")

fig_scatter = px.scatter(
    filtered_titles,
    x="popularity",
    y="revenue",
    color="release_year",
    hover_data=["title", "genres", "vote_average", "vote_count"],
    title="Popularité vs Revenu des films par titre",
    labels={
        "popularity": "Indice de popularité",
        "revenue": "Revenu",
        "release_year": "Année"
    }
)

st.plotly_chart(fig_scatter, width="stretch")

st.subheader("💰 Revenu moyen par genre")

genre_revenue = (
    filtered_genres
    .groupby("genre_name", as_index=False)
    .agg(
        revenu_moyen=("revenue", "mean"),
        popularite_moyenne=("popularity", "mean"),
        nombre_films=("id", "nunique")
    )
    .sort_values("revenu_moyen", ascending=False)
)

if genre_revenue.empty:
    st.info("Aucune donnée par genre pour les filtres sélectionnés.")
else:
    fig_bar_revenue = px.bar(
        genre_revenue,
        x="genre_name",
        y="revenu_moyen",
        hover_data=["popularite_moyenne", "nombre_films"],
        title="Revenu moyen par genre",
        labels={
            "genre_name": "Genre",
            "revenu_moyen": "Revenu moyen"
        }
    )

    st.plotly_chart(fig_bar_revenue, width="stretch")

st.subheader("🔥 Popularité moyenne par genre")

if genre_revenue.empty:
    st.info("Aucune donnée de popularité par genre pour les filtres sélectionnés.")
else:
    fig_bar_popularity = px.bar(
        genre_revenue.sort_values("popularite_moyenne", ascending=False),
        x="genre_name",
        y="popularite_moyenne",
        hover_data=["revenu_moyen", "nombre_films"],
        title="Popularité moyenne par genre",
        labels={
            "genre_name": "Genre",
            "popularite_moyenne": "Popularité moyenne"
        }
    )

    st.plotly_chart(fig_bar_popularity, width="stretch")

st.subheader("📅 Évolution des revenus et de la popularité par année")

yearly_data = (
    filtered_genres
    .groupby(["release_year", "genre_name"], as_index=False)
    .agg(
        revenu_moyen=("revenue", "mean"),
        popularite_moyenne=("popularity", "mean"),
        nombre_films=("id", "nunique")
    )
)

if yearly_data.empty:
    st.info("Aucune évolution par année pour les filtres sélectionnés.")
else:
    fig_line_revenue = px.line(
        yearly_data,
        x="release_year",
        y="revenu_moyen",
        color="genre_name",
        markers=True,
        title="Évolution du revenu moyen par genre",
        labels={
            "release_year": "Année",
            "revenu_moyen": "Revenu moyen",
            "genre_name": "Genre"
        }
    )

    st.plotly_chart(fig_line_revenue, width="stretch")

    fig_line_popularity = px.line(
        yearly_data,
        x="release_year",
        y="popularite_moyenne",
        color="genre_name",
        markers=True,
        title="Évolution de la popularité moyenne par genre",
        labels={
            "release_year": "Année",
            "popularite_moyenne": "Popularité moyenne",
            "genre_name": "Genre"
        }
    )

    st.plotly_chart(fig_line_popularity, width="stretch")

st.subheader("🧠 Corrélation popularité / revenu par genre")

correlation_by_genre = (
    filtered_genres
    .groupby("genre_name")
    .apply(lambda x: safe_correlation(x, "popularity", "revenue"))
    .reset_index(name="correlation")
    .dropna()
    .sort_values("correlation", ascending=False)
)

if correlation_by_genre.empty:
    st.info("Pas assez de données pour calculer une corrélation par genre.")
else:
    fig_corr = px.bar(
        correlation_by_genre,
        x="genre_name",
        y="correlation",
        title="Corrélation popularité / revenu selon le genre",
        labels={
            "genre_name": "Genre",
            "correlation": "Corrélation"
        }
    )

    st.plotly_chart(fig_corr, width="stretch")
    st.dataframe(correlation_by_genre, width="stretch")

st.subheader("🏆 Top films")

col_left, col_right = st.columns(2)

top_revenue = (
    filtered_titles
    .sort_values("revenue", ascending=False)
    [["title", "release_year", "genres", "popularity", "revenue", "vote_average"]]
    .head(10)
)

top_popularity = (
    filtered_titles
    .sort_values("popularity", ascending=False)
    [["title", "release_year", "genres", "popularity", "revenue", "vote_average"]]
    .head(10)
)

with col_left:
    st.markdown("### Top 10 par revenu")
    st.dataframe(top_revenue, width="stretch")

with col_right:
    st.markdown("### Top 10 par popularité")
    st.dataframe(top_popularity, width="stretch")

st.subheader("🔎 Analyse par titre")

title_analysis = (
    filtered_titles
    .sort_values("revenue", ascending=False)
    [[
        "title",
        "release_year",
        "genres",
        "revenue",
        "popularity",
        "revenue_per_popularity_point",
        "vote_average",
        "vote_count",
    ]]
)

st.dataframe(title_analysis, width="stretch")

st.subheader("📝 Interprétation")

if correlation is None:
    st.info("La corrélation n'est pas calculable avec les filtres actuels.")
elif correlation > 0.7:
    st.success("La corrélation est forte : les films les plus populaires ont généralement les revenus les plus élevés.")
elif correlation > 0.4:
    st.info("La corrélation est modérée : la popularité influence les revenus, mais d'autres facteurs jouent aussi un rôle.")
elif correlation > 0.1:
    st.warning("La corrélation est faible : la popularité semble avoir une influence limitée sur les revenus.")
else:
    st.error("La corrélation est très faible ou inexistante : la popularité seule n'explique pas les revenus.")
