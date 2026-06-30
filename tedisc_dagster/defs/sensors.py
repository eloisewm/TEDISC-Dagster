from dagster import RunRequest, sensor, SensorEvaluationContext
from tedisc_dagster.defs.jobs import birdlife_job
import os
from dotenv import load_dotenv

load_dotenv()
filepath = os.environ.get("BLA_FILEPATH")

@sensor(job=birdlife_job, minimum_interval_seconds=3600)  # polls every hour
def bla_new_data_sensor(context: SensorEvaluationContext):

    if not filepath or not os.path.exists(filepath):
        context.log.warning(f"BLA file not found at {filepath}")
        return

    current_size = os.path.getsize(filepath)
    last_size = int(context.cursor) if context.cursor else None

    if last_size is None:
        context.log.info(f"First poll — recording file size: {current_size} bytes. No run triggered.")
        context.update_cursor(str(current_size))
        return

    if current_size != last_size:
        context.log.info(f"File size changed ({last_size} -> {current_size} bytes). Triggering run.")
        context.update_cursor(str(current_size))
        yield RunRequest(run_key=str(current_size))
    else:
        context.log.info(f"No change in file size ({current_size} bytes). No run triggered.")