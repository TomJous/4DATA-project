from dagster import ScheduleDefinition
from ..jobs import movie_monthly_job

movie_monthly_schedule = ScheduleDefinition(
    name="movie_monthly_schedule",
    job=movie_monthly_job,
    cron_schedule="0 2 1 * *",
    execution_timezone="Europe/Paris",
)