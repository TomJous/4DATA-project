from dagster import define_asset_job,AssetSelection,in_process_executor

from ..partitions import monthly_partition

movie_monthly_job = define_asset_job(
    name="movie_monthly_job",
    selection=AssetSelection.all(),
    executor_def=in_process_executor,
    partitions_def=monthly_partition,
)