import os
import tempfile
from typing import List

import geopandas as gpd
import requests
from google.cloud import bigquery

from eoflow.models import DataSpec, S2IndexDF, Tile


def get_tiles(dataspec: DataSpec) -> list[Tile]:

    CATALOGUE_BASE_URL = os.getenv(
        "CATALOGUE_BASE_URL", "https://eo-catalogue.svante.io"
    )
    CATALOGUE_API_KEY = os.getenv("CATALOGUE_API_KEY")
    print("HELO APO KEY", CATALOGUE_API_KEY)

    tmp = tempfile.NamedTemporaryFile(suffix=".gpkg")

    # TODO: handle cloud geofile
    gdf = gpd.read_file(dataspec.target_geofile)

    # TODO: handle too-large geofiles
    gdf.to_file(tmp.name, driver="gpkg")

    headers = {"x-api-key": CATALOGUE_API_KEY}

    r = requests.post(
        CATALOGUE_BASE_URL + "/mgrs-tiles",
        files={"file": (tmp.name, open(tmp.name, "rb"))},
        headers=headers,
    )

    r.raise_for_status()

    return [Tile(**item) for item in r.json()["tiles"]]


def get_revisits(tiles: List[Tile], dataspec: DataSpec) -> S2IndexDF:

    query_columns = ",".join(
        [
            key
            for key in S2IndexDF.to_json_schema()["properties"].keys()
            if key != "index"
        ]
    )
    query_tiles = ",".join([f"'{tile.tile}'" for tile in tiles])

    Q = f"""
        SELECT
          {query_columns}
        FROM `bigquery-public-data.cloud_storage_geo_index.sentinel_2_sr_index`
        WHERE sensing_time > '{dataspec.start_datetime}'
        AND sensing_time <= '{dataspec.end_datetime}'
        AND mgrs_tile IN ({query_tiles})
        LIMIT 3
    """

    client = bigquery.Client()
    query_job = client.query(Q)

    results = query_job.result()  # Waits for job to complete.

    df = results.to_dataframe()

    return df
