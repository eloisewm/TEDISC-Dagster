from dagster import asset, AssetExecutionContext, Failure
import pandas as pd
from tedisc_dagster.defs import constants

import os
from pathlib import Path
from datetime import datetime, date
from pyinaturalist import get_observations

# Currently I run out of memory before this asset can execute
# Will need to either:
#   1) Save each species to disk as I go through them in the loop
#   2) Write an asset for each individual species 
# Decision will impact how I write future assets - must decide before moving on
import os
import pandas as pd
from pyinaturalist import get_observations

from dagster import asset, AssetExecutionContext, MetadataValue

from tedisc_dagster.defs import constants


@asset(
    group_name="iNaturalist",
    description="Raw research-grade observations fetched from the iNaturalist API for all target species, covering the Southern Hemisphere.",
)
def raw_obs_inat(context: AssetExecutionContext) -> pd.DataFrame:

    output_dir = "inat_raw"
    os.makedirs(output_dir, exist_ok=True)

    species_record_counts = {}

    for taxon_name in constants.ALL_TARGET_SPECIES_SCIENTIFIC:

        count_response = get_observations(
            taxon_name=taxon_name,
            quality_grade="research",
            count_only=True,
            nelat=0,
            swlat=-90,
            nelng=180,
            swlng=-180,
        )
        total = count_response["total_results"]
        context.log.info(f"[{taxon_name}] Total observations to download: {total}")

        page = 1
        per_page = 200
        observations = []

        while True:
            response = get_observations(
                taxon_name=taxon_name,
                quality_grade="research",
                per_page=per_page,
                page=page,
                nelat=0,
                swlat=-90,
                nelng=180,
                swlng=-180,
            )

            results = response["results"]
            if not results:
                break

            observations.extend(results)
            context.log.info(f"[{taxon_name}] Page {page} - {len(observations)} records so far")
            page += 1

        df = pd.json_normalize(observations)
        safe_name = taxon_name.replace(" ", "_")
        df.to_csv(f"{output_dir}/{safe_name}.csv", index=False)
        context.log.info(f"[{taxon_name}] Saved {len(df)} rows to {output_dir}/{safe_name}.csv")

        species_record_counts[taxon_name] = len(df)

    # Read all CSV files back and concatenate
    files = [f"{output_dir}/{f}" for f in os.listdir(output_dir) if f.endswith(".csv")]
    df_all = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    total_rows = len(df_all)
    context.log.info(f"Total rows across all species: {total_rows}")

    context.add_output_metadata({
        "row_count": MetadataValue.int(total_rows),
        "column_count": MetadataValue.int(len(df_all.columns)),
        "species_count": MetadataValue.int(len(species_record_counts)),
        "records_per_species": MetadataValue.json(species_record_counts),
        "preview": MetadataValue.md(df_all.head(5).to_markdown()),
    })

    return df_all