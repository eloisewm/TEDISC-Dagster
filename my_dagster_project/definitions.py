from dagster import Definitions

from my_dagster_project.defs.assets import raw_obs_bla, validated_structure_bla, deduplicated_bla, flagged_data_bla, typed_bla, padded_bla, raw_obs_inat
from my_dagster_project.defs.jobs import inventory_job
from my_dagster_project.defs.schedules import daily_inventory_schedule
from my_dagster_project.defs.sensors import new_data_sensor

# This is the single object Dagster reads when it starts up.
# Everything you want Dagster to know about must be listed here.

defs = Definitions(
    assets=[raw_obs_bla, validated_structure_bla, deduplicated_bla, flagged_data_bla, typed_bla, padded_bla, raw_obs_inat],
    jobs=[inventory_job],
    schedules=[daily_inventory_schedule],
    sensors=[new_data_sensor],
)

