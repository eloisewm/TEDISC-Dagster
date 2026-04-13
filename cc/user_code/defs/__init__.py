from dagster import Definitions, asset, define_asset_job, ScheduleDefinition


@asset
def example_asset():
    """An example asset that returns a simple value."""
    return [1, 2, 3]


@asset
def downstream_asset(example_asset):
    """An asset that depends on example_asset."""
    return sum(example_asset)


example_job = define_asset_job(name="example_job")

example_schedule = ScheduleDefinition(
    job=example_job,
    cron_schedule="0 * * * *",  # every hour
)

defs = Definitions(
    assets=[example_asset, downstream_asset],
    jobs=[example_job],
    schedules=[example_schedule],
)
