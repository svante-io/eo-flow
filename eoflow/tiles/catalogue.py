import os
import tempfile

import geopandas as gpd
import requests

from eoflow.models import DataSpec, Tile


def get_tiles(dataspec: DataSpec) -> list[Tile]:

    CATALOGUE_BASE_URL = os.getenv(
        "CATALOGUE_BASE_URL", "https://eo-catalogue.svante.io"
    )
    CATALOGUE_API_KEY = os.getenv("CATALOGUE_API_KEY")

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
