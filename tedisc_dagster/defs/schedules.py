from dagster import ScheduleDefinition

from tedisc_dagster.defs.jobs import inventory_job

# Runs the inventory_job every day at 6:00 AM.
# Cron format: minute hour day-of-month month day-of-week
# "0 6 * * *" = 06:00 every day

daily_inventory_schedule = ScheduleDefinition(
    job=inventory_job,
    cron_schedule="0 6 * * *",
)