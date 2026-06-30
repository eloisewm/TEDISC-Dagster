from dagster import AssetSelection, define_asset_job

birdlife_job = define_asset_job(
    name="birdlife_job",
    selection=AssetSelection.assets(
        "raw_obs_bla", 
        "validated_structure_bla", 
        "deduplicated_bla", 
        "flagged_data_bla", 
        "typed_bla", 
        "padded_bla", 
        "exported_bla"
        ),
)

inat_job = define_asset_job(
    name="inat_job",
    selection=AssetSelection.assets("raw_obs_inat"),
)