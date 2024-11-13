import pytest
from dagster import RunConfig

from eoflow.dag.materialize import materialize_local
from eoflow.models import DataSpec


def test_materialize_local_basic():

    dataspec = DataSpec(
        target_geofile="tests/data/parks.geojson",
        dataset_store="data",
        bands=["B01", "B09"],
    )

    run_cfg = {
        "get_tiles_op": dataspec,
        "dynamic_revisits": dataspec,
        "op_materialize_tile": dataspec,
        "op_merge_and_store_dataset_index": dataspec,
    }

    assert materialize_local.execute_in_process(run_config=RunConfig(run_cfg)).success


@pytest.mark.skip(reason="expensive.")
def test_materialize_local_extensive():
    dataspec = DataSpec(
        target_geofile="gs://eo-flow-public/dev/test-data/london_greenspaces.geojson",
        dataset_store="data/local_store",
        start_datetime="2024-10-01",
        end_datetime="2024-10-27",
        bands=["B04", "B03", "B02"],
        chipsize=180,
    )

    run_cfg = {
        "get_tiles_op": dataspec,
        "dynamic_revisits": dataspec,
        "op_materialize_tile": dataspec,
        "op_merge_and_store_dataset_index": dataspec,
    }

    assert materialize_local.execute_in_process(run_config=RunConfig(run_cfg)).success
