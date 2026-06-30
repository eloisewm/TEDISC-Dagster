from dagster import ScheduleDefinition

from tedisc_dagster.defs.jobs import birdlife_job, inat_job 

# Runs the birdlife_job and inat_job every day at 6:00 AM.
# Cron format: minute hour day-of-month month day-of-week
# "0 6 * * *" = 06:00 every day

daily_birdlife_schedule = ScheduleDefinition(
    job=birdlife_job,
    cron_schedule="0 6 * * *",
)

daily_inat_schedule = ScheduleDefinition(
    job=inat_job,
    cron_schedule="0 6 * * *",
)