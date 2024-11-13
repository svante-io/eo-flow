import json

import pytest

from eoflow.models import DataSpec, S2IndexItem, Tile


@pytest.fixture
def sample_dataspec():
    return DataSpec(
        target_geofile="tests/data/parks.geojson",
        dataset_store="tests/data/local_store",
        bands=["B01", "B09"],
    )


@pytest.fixture
def sample_archive_tile():
    return Tile(tile="30UXC")


@pytest.fixture
def sample_archive_revisits():
    archive_raw = json.load(open("./tests/data/sample_index_items.json"))
    return [
        S2IndexItem(**item) for item in archive_raw if item.get("mgrs_tile") == "30UXC"
    ]
