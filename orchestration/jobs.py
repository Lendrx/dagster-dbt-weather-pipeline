from dagster import ScheduleDefinition, define_asset_job

# ------------------------------------------------------------------------------------
# Job Definition
# ------------------------------------------------------------------------------------

daily_elt_job = define_asset_job(
    name="daily_elt_job",
    description="Fuehrt Ingestion und dbt Pipeline zusammen aus.",
    selection="*",
)

# ------------------------------------------------------------------------------------
# Schedule Definition
# ------------------------------------------------------------------------------------

daily_elt_schedule = ScheduleDefinition(
    job=daily_elt_job,
    cron_schedule="*/30 * * * *",
)
