from setuptools import find_packages, setup

setup(
    name="dagster_university",
    packages=find_packages(exclude=["dagster_university_tests"]),
    install_requires=[
        "dagster==1.7.*",
        "dagster-cloud",
        "geopandas",
        "kaleido",
        "pandas",
        "plotly",
        "psycopg2-binary",
        "shapely",
        "sqlalchemy",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
