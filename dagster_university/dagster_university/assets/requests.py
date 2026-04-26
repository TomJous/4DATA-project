from dagster import Config

class MovieConfig(Config):
    start_date: str = "2025-03-03" #dates à modifier pour changer la périodicité du pipeline
    end_date: str = "2025-04-04" 