from dagster import Definitions

from tedisc_dagster.defs.assets import bla_assets, inat_assets
from tedisc_dagster.defs.jobs import inventory_job
from tedisc_dagster.defs.schedules import daily_inventory_schedule
from tedisc_dagster.defs.sensors import new_data_sensor

# This is the single object Dagster reads when it starts up.
# Everything you want Dagster to know about must be listed here.

defs = Definitions(
    assets=[*bla_assets, *inat_assets],
    jobs=[inventory_job],
    schedules=[daily_inventory_schedule],
    sensors=[new_data_sensor],
)

