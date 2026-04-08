from dagster import asset, Definitions


@asset
def hello_world():
    return "Hello, Dagster"

defs = Definitions(assets=[hello_world])
