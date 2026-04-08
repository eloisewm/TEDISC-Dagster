import csv
from io import StringIO

import httpx
from dagster import Definitions, asset


@asset
def wfs_inventory() -> str:
    url = 'https://geoserver.imas.utas.edu.au/geoserver/NESP/ows'
    # You could just include these in the url, I'm just being pedantic here:
    params = {
        'service': 'WFS',
        'version': '1.0.0',
        'request': 'GetFeature',
        'typeName': 'NESP:NESP_MaC_3_21_ORE_prioritySpp_data_inventory',
        'outputFormat': 'csv',
    }
    res = httpx.get(url, params=params)
    return res.text


@asset
def save_features(wfs_inventory: str) -> int:
    data_str = StringIO(wfs_inventory)
    reader = csv.DictReader(data_str)
    # print(reader.fieldnames)
    #
    return len(list(reader))

@asset
def hello_world():
    return "Hello, Dagster"

defs = Definitions(assets=[hello_world, wfs_inventory, save_features])
