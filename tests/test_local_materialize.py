from dagster import RunConfig

from eoflow.dag.materialize import materialize_local
from eoflow.models import DataSpec


def test_materialize_local():

    dataspec = DataSpec(
        target_geofile="tests/data/aoi.geojson",
    )

    run_cfg = {
        "get_tiles_op": dataspec,
        "dynamic_revisits": dataspec,
    }

    assert materialize_local.execute_in_process(run_config=RunConfig(run_cfg)).success
