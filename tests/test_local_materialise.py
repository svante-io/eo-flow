from dagster import RunConfig

from eoflow.dag.materialise import materialise_local
from eoflow.models import DataSpec


def test_materialise_local():

    dataspec = DataSpec(
        target_geofile="tests/data/aoi.geojson",
    )

    run_cfg = {
        "get_tiles_op": dataspec,
        "dynamic_revisits": dataspec,
    }

    assert materialise_local.execute_in_process(run_config=RunConfig(run_cfg)).success
