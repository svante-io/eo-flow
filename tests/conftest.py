import json

import pytest

from eoflow.models import DataSpec, S2IndexItem, Tile


@pytest.fixture
def sample_dataspec():
    return DataSpec(
        target_geofile="tests/data/aoi.geojson",
        bands=["B01", "B09"],
    )


@pytest.fixture
def sample_archive_tile():
    return Tile(tile="30UXC")


@pytest.fixture
def sample_archive_revisits():
    archive_raw = json.load(open("./tests/data/sample_revisits.json"))
    return [S2IndexItem(**item) for item in archive_raw if item.get("30UXC")]
