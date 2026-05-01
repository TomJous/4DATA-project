from dagster import MonthlyPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date=start_date
)