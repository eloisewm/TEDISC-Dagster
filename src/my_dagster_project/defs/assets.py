import csv
from io import StringIO

import httpx
from dagster import asset


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