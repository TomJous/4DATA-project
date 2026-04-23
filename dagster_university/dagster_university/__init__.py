# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import metrics, movies

trip_assets = load_assets_from_modules([movies])
metric_assets = load_assets_from_modules([metrics])

defs = Definitions(
    assets=[*trip_assets, *metric_assets]
)
