from dagster import Definitions

from tedisc_dagster.defs.assets import bla_assets, inat_assets
from tedisc_dagster.defs.jobs import inat_job, birdlife_job
from tedisc_dagster.defs.schedules import daily_birdlife_schedule, daily_inat_schedule
from tedisc_dagster.defs.sensors import bla_new_data_sensor
from tedisc_dagster.defs.resources import bcp_io_manager

# This is the single object Dagster reads when it starts up.
# Everything you want Dagster to know about must be listed here.

defs = Definitions(
    assets=[*bla_assets, *inat_assets],
    jobs=[inat_job, birdlife_job],
    schedules=[daily_birdlife_schedule, daily_inat_schedule],
    sensors=[bla_new_data_sensor],
    resources={"bcp_io_manager": bcp_io_manager}
)

