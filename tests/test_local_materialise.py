from dagster import RunConfig

from eoflow.dag.materialise import materialise_local
from eoflow.models import DataSpec


def test_materialise_local():

    dataspec = DataSpec(
        target_geofile="tests/data/sample.geojson",
    )

    run_cfg = {
        "dynamic_tiles": dataspec,
        "materialise_tile": dataspec,
    }

    assert materialise_local.execute_in_process(run_config=RunConfig(run_cfg)).success
