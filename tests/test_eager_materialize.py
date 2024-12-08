from dagster import RunConfig

from eoflow.dag.materialize import materialize_eager
from eoflow.models import DataSpec


def test_materialize_eager_extensive():
    dataspec = DataSpec(
        target_geofile="gs://eo-flow-public/dev/test-data/parks.geojson",
        dataset_store="gs://eo-flow-dev/initial_tests",
        start_datetime="2024-10-01",
        end_datetime="2024-10-27",
        bands=["B01", "B09"],
        chipsize=180,
    )

    run_cfg = {
        "get_tiles_op": dataspec,
        "op_revisits": dataspec,
        "op_materialize_tile_eager": dataspec,
        "op_collect_and_merge_indices": dataspec,
    }

    assert materialize_eager.execute_in_process(run_config=RunConfig(run_cfg)).success
