import os
from dagster import sensor, RunRequest, SkipReason
from ..jobs import movie_monthly_job

DATA_DIR = "data/raw/"

@sensor(job=movie_monthly_job)
def movie_file_sensor(context):

    files = sorted([
        f for f in os.listdir(DATA_DIR)
        if f.startswith("movies_") and f.endswith(".json")
    ])

    if not files:
        return SkipReason("Aucun fichier movies_*.json trouvé.")

    latest_file = files[-1]

    if context.cursor == latest_file:
        return SkipReason("Aucun nouveau fichier détecté.")

    context.update_cursor(latest_file)

    partition_key = latest_file.split("_")[1]

    return RunRequest(
        run_key=latest_file,
        partition_key=partition_key,
        tags={
            "source": "movie_file_sensor",
            "file_name": latest_file
        }
    )