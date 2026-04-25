import os
from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource, dbt_assets
from orchestration.assets import (
    raw_sales_data,
    raw_weather_hourly_data,
    weather_raw_freshness_check,
    weather_raw_min_rows_check,
)
from orchestration.jobs import daily_elt_job, daily_elt_schedule

# ------------------------------------------------------------------------------------
# Pfade und Ressourcen
# ------------------------------------------------------------------------------------

DBT_PROJECT_DIR = (Path(__file__).parent.parent / "dbt_project").resolve()
DBT_PROFILES_DIR = (Path.home() / ".dbt").resolve()

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROFILES_DIR),
    profile="dbt_project",
    target="dev",
)


# ------------------------------------------------------------------------------------
# dbt Assets
# ------------------------------------------------------------------------------------

@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def my_dbt_assets(context, dbt: DbtCliResource):
    # dbt Build fuer den kompletten Graph.
    yield from dbt.cli(["build"], context=context).stream()


# ------------------------------------------------------------------------------------
# Dagster Definitions
# ------------------------------------------------------------------------------------

defs = Definitions(
    assets=[raw_sales_data, raw_weather_hourly_data, my_dbt_assets],
    asset_checks=[weather_raw_min_rows_check, weather_raw_freshness_check],
    jobs=[daily_elt_job],
    schedules=[daily_elt_schedule],
    resources={"dbt": dbt_resource},
)
