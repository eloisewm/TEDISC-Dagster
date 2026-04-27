
from dagster import asset, AssetExecutionContext, Failure
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

    # Load in data as a dataframe
    df = pd.read_csv(filepath)

    # Record size of the file and the date last modified
    path = Path(filepath)
    file_size_mb = os.path.getsize(path) / (1024 * 1024) # in bytes, convert to mb
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
def validated_structure(context: AssetExecutionContext, raw_obs: pd.DataFrame) -> pd.DataFrame:
    """
    Check that raw_obs has the expected column structure.
    Does not modify raw_obs - check only.
    Logs missing and additional columns for reference.
    """
    expected = set(EXPECTED_BLA_COLUMNS)
    actual = set(raw_obs.columns)
    missing_cols = expected - actual
    additional_cols = actual - expected
 

    if missing_cols:
        raise Failure(description=f"MISSING COLUMNS ({len(missing_cols)}): {sorted(missing_cols)}")  
    else:
        context.log.info("No missing columns — all expected columns present.")

    if additional_cols:
        raise Failure(description=f"UNEXPECTED COLUMNS ({len(additional_cols)}): {sorted(additional_cols)}")

    else:
        context.log.info("No additional columns — file matches expected schema exactly.")

    return raw_obs 


@asset(group_name = "BirdLife_Australia")
def deduplicated(context: AssetExecutionContext, validated_structure: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows from raw observations."""
    original_count = len(validated_structure)
    df = validated_structure.drop_duplicates()
    no_of_dupes = original_count - len(df)

    if no_of_dupes:
        context.log.info(f"{no_of_dupes} duplicate rows removed")
    else:
        context.log.info("No duplicate rows found")

    return df


# @asset(group_name="BirdLife_Australia")
# def vocab_checked(context: AssetExecutionContext, validate_structure: pd.DataFrame) -> pd.DataFrame:
#     """
#     Validates that column values conform to expected types, ranges, and controlled vocabularies.
#     Logs warnings for violations but does not halt the pipeline — violations are flagged for review.
#     Returns the dataframe unchanged.
#     """
#     df = validate_structure





        


