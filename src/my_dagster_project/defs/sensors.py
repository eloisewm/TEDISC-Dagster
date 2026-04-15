from dagster import RunRequest, sensor

from my_dagster_project.jobs import inventory_job


# A sensor watches for some external condition and triggers a job run when it's met.
# This one is stubbed out — it never actually fires — but shows you the pattern.
#
# A real sensor might check: did a new file land in S3? Did a database table update?
# If yes, yield a RunRequest to kick off the job.

@sensor(job=inventory_job)
def new_data_sensor(context):
    # Replace this with real logic, e.g. check an S3 bucket or a database timestamp.
    new_data_available = False

    if new_data_available:
        yield RunRequest(run_key="example_run")