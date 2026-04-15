from dagster import Definitions

from my_dagster_project.assets import save_features, wfs_inventory
from my_dagster_project.jobs import inventory_job
from my_dagster_project.schedules import daily_inventory_schedule
from my_dagster_project.sensors import new_data_sensor

# This is the single object Dagster reads when it starts up.
# Everything you want Dagster to know about must be listed here.

defs = Definitions(
    assets=[wfs_inventory, save_features],
    jobs=[inventory_job],
    schedules=[daily_inventory_schedule],
    sensors=[new_data_sensor],
)