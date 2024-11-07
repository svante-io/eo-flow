import os
import tempfile
from functools import reduce
from typing import List

import requests
from google.cloud import bigquery

from eoflow.core.logging import logger
from eoflow.core.utils import read_any_geofile
from eoflow.models import DataSpec, S2IndexDF, Tile


def get_tiles(dataspec: DataSpec, logger=logger) -> list[Tile]:

    CATALOGUE_BASE_URL = os.getenv(
        "CATALOGUE_BASE_URL", "https://eo-catalogue.svante.io"
    )
    CATALOGUE_API_KEY = os.getenv("CATALOGUE_API_KEY")

    tmp = tempfile.NamedTemporaryFile(suffix=".gpkg")

    gdf = read_any_geofile(dataspec.target_geofile)

    if len(gdf) > 1000000:
        raise ValueError("Too many rows in target geofile to use tiling service")

    BATCH = 5000  # toDO move to env var and match catalog service

    tiles = []

    for ii in range(len(gdf) // BATCH + 1):

        logger.info(f"Fetching tiles: {ii + 1}/{len(gdf) // BATCH + 1}")

        gdf.iloc[ii * BATCH : (ii + 1) * BATCH].to_file(tmp.name, driver="gpkg")

        headers = {"x-api-key": CATALOGUE_API_KEY}

        r = requests.post(
            CATALOGUE_BASE_URL + "/mgrs-tiles",
            files={"file": (tmp.name, open(tmp.name, "rb"))},
            headers=headers,
        )

        r.raise_for_status()

        tiles += [Tile(**item) for item in r.json()["tiles"]]

    unique_tiles = reduce(
        lambda x, y: x + [y] if (y.tile not in {t.tile for t in x}) else x, tiles, []
    )

    return unique_tiles


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
