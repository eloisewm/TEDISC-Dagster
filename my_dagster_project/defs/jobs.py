from dagster import AssetSelection, define_asset_job

# This job runs both assets in order: wfs_inventory first, then save_features.
# AssetSelection.all() grabs every asset registered in definitions.py.
# You could also be specific: AssetSelection.keys("wfs_inventory", "save_features")

inventory_job = define_asset_job(
    name="inventory_job",
    selection=AssetSelection.all(),
)