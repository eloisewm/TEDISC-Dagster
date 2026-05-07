from dagster import asset, AssetExecutionContext, Failure
import pandas as pd
from tedisc_dagster.defs import constants

import os
from pathlib import Path
from datetime import datetime, date
from pyinaturalist import get_observations

# ───────────────────────────────────────────────────────────────────────────────────────────────────
# iNaturalist
# ───────────────────────────────────────────────────────────────────────────────────────────────────

# Currently I run out of memory before this asset can execute
# Will need to either:
#   1) Save each species to disk as I go through them in the loop
#   2) Write an asset for each individual species 
# Decision will impact how I write future assets - must decide before moving on
@asset(group_name="iNaturalist")
def raw_obs_inat(context: AssetExecutionContext) -> pd.DataFrame:

#     all_observations = []

#     for taxon_name in constants.ALL_TARGET_SPECIES_SCIENTIFIC:

#         # Count the number of obs that we are about to request
#         count_response = get_observations(
#             taxon_name=taxon_name,
#             quality_grade="research",
#             count_only=True, 
#             nelat=0,     
#             swlat=-90,    
#             nelng=180,    
#             swlng=-180,
#         )
#         total = count_response["total_results"]
#         context.log.info(f"[{taxon_name}] Total observations to download: {total}")

#         page = 1        # Will iterate in the loop over all possible pages
#         per_page = 200  # Max we can do

#         while True:
#             # API pull for all reasearch grade observations in the southern hemiphere
#             response = get_observations(
#                 taxon_name=taxon_name,
#                 quality_grade="research",
#                 per_page=per_page,
#                 page=page,
#                 nelat=0,      
#                 swlat=-90,    
#                 nelng=180,   
#                 swlng=-180,
#             )

#             results = response["results"]
#             if not results:
#                 break

#             all_observations.extend(results)
#             context.log.info(f"[{taxon_name}] Page {page} - {len(all_observations)} total records so far")
#             page += 1

#     df = pd.json_normalize(all_observations)

#     context.add_output_metadata({
#         "row_count": len(df),
#         "column_count": len(df.columns),
#         "species_count": len(constants.ALL_TARGET_SPECIES_SCIENTIFIC),
#         "quality_grade": "research",
#     })

#     return df
    
    return None