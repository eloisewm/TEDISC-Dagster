
from dagster import asset, AssetExecutionContext
import pandas as pd
from .constants import EXPECTED_BLA_COLUMNS

import os
from pathlib import Path
from datetime import datetime


###### BirdLife Australia QAQC ##########

filepath = "/home/eloisewm/Datasets/BirdLife Aus/bla_output_csiro_290525.csv"

@asset(
        group_name = "BirdLife_Australia", 
        metadata = {
           "source": "Birdlife Australia",
           "license": "CC-BY 4.0"}
)
def raw_obs(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw BirdLife Australia observations from CSV."""
    df = pd.read_csv(filepath)

    # Record size of the file and the date last modified
    path = Path(filepath)
    file_size_mb = os.path.getsize(path) / (1024 * 1024)
    last_modified = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%d/%m/%Y")

    # Add metadata for this materialisation of the file
    context.add_output_metadata({
        "row_count": len(df),
        "column_count": len(df.columns),
        "filepath": filepath, 
        "file_size_mb": file_size_mb,
        "last_modified": last_modified
    })
    
    return df


@asset(group_name = "BirdLife_Australia")
def validate_structure(context: AssetExecutionContext, raw_obs: pd.DataFrame) -> None:
    """
    Check that raw_obs has the expected column structure.
    Does not modify raw_obs — check only.
    Logs missing and additional columns for reference.
    """
    expected = set(EXPECTED_BLA_COLUMNS)
    actual = set(raw_obs.columns)
    missing_cols = expected - actual
    additional_cols = actual - expected
    if missing_cols:
        context.log.warning(
            f"MISSING columns ({len(missing_cols)}): {sorted(missing_cols)}"
        )    
    else:
        context.log.info("No missing columns — all expected columns present.")

    if additional_cols:
        context.log.warning(
            f"ADDITIONAL columns ({len(additional_cols)}): {sorted(additional_cols)}"
        )
    else:
        context.log.info("No additional columns — file matches expected schema exactly.")

    return     



        


