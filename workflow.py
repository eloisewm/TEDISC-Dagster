import csv
from io import StringIO

import httpx
import pyodbc
from dagster import AutomationCondition, Definitions, asset


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


@asset(automation_condition=AutomationCondition.eager())
def save_features(wfs_inventory: str) -> int:
    data_str = StringIO(wfs_inventory)
    # I think MSSQL has a bulk-insert method directly from CSV!  You might have issues with the filepath and the server though.
    # reader = csv.DictReader(data_str)

    # I like using DictReader most of the time, but for simplicity
    # I'll just use a regular reader so we can bulk-load it (assuming
    # the db table has identical structure to the csv):

    # print(reader.fieldnames)
    reader = csv.reader(wfs_inventory)
    # cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};Server=imasoredev-db.its.utas.edu.au;Database=IMASORE;UID=username;PWD=password")
    # data = list(reader)
    # data = data[1:]             # skip header row
    # with cnxn.cursor() as cursor:
    #     cursor.executemany('INSERT INTO ore_inventory (FID, species_group, common_name, scientific_name, source_data_repo, data_subset, data_type, observation_count, location, years, data_owner, data_owner_affiliation, access_url, publications, notes) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data)

    # Just return an integer, for funsies:
    return len(list(reader))


defs = Definitions(assets=[wfs_inventory, save_features])
