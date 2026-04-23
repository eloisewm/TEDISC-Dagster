import csv
from io import StringIO

import httpx

from dagster import asset, AssetExecutionContext
import pandas as pd
from .constants import EXPECTED_BLA_COLUMNS


@asset
def wfs_inventory() -> str:
    """Fetches the ORE priority species inventory from the IMAS GeoServer WFS."""
    url = "https://geoserver.imas.utas.edu.au/geoserver/NESP/ows"
    params = {
        "service": "WFS",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": "NESP:NESP_MaC_3_21_ORE_prioritySpp_data_inventory",
        "outputFormat": "csv",
    }
    res = httpx.get(url, params=params)
    return res.text


@asset
def save_features(wfs_inventory: str) -> int:
    """Reads the WFS CSV and returns a row count. Database insert is stubbed out."""
    reader = csv.reader(StringIO(wfs_inventory))
    rows = list(reader)
    row_count = len(rows) - 1  # subtract header row
    print(f"Found {row_count} features in the inventory.")
    return row_count



###### BirdLife Australia QAQC ##########

filepath = "/home/eloisewm/Datasets/BirdLife Aus/bla_output_csiro_290525.csv"

@asset(
        group_name = "BirdLife_Australia", 
        metadata = {
           "source": "Birdlife Australia",
           "download_date": "29/05/2025"}
)
def raw_obs(context: AssetExecutionContext) -> pd.DataFrame:
    """Load raw BirdLife Australia observations from CSV."""
    df = pd.read_csv(filepath)
    context.log.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {filepath}")
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


  # TEST
        


