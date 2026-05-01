# fmt: off
from dagster import Definitions, in_process_executor, load_assets_from_modules

from .assets import metrics, movies
from .resources import database_resource
from .jobs import movie_monthly_job
from .schedules import movie_monthly_schedule
from .sensors import movie_file_sensor

trip_assets = load_assets_from_modules([movies])
metric_assets = load_assets_from_modules([metrics])

defs = Definitions(
    assets=[*trip_assets, *metric_assets],
    jobs=[movie_monthly_job],
    schedules=[movie_monthly_schedule],
    sensors=[movie_file_sensor],
    resources={
        "database": database_resource
    },
    executor=in_process_executor,
)
