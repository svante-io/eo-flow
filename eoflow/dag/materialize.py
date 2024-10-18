from dagster import DynamicOut, DynamicOutput, In, Out, graph, op
from dagster_pandera import pandera_schema_to_dagster_type

from eoflow.lib.catalogue import get_revisits, get_tiles
from eoflow.models import DataSpec, S2IndexDF, Tile

DagsterS2IndexDF = pandera_schema_to_dagster_type(S2IndexDF)


@op(out=Out())
def get_tiles_op(config: DataSpec):
    return get_tiles(config)


@op(ins={"tiles": In(list[Tile])}, out=DynamicOut(dagster_type=DagsterS2IndexDF))
def dynamic_revisits(tiles: list[Tile], config: DataSpec):
    df_revisits = get_revisits(tiles, config)

    for mgrs_tile, df_revisit_slice in df_revisits.groupby("mgrs_tile"):
        yield DynamicOutput(df_revisit_slice, mapping_key=mgrs_tile)


@op(ins={"df_revisit_slice": In(DagsterS2IndexDF)}, out=Out())
def op_materialize_tile_run_job(df_revisit_slice: S2IndexDF, config: DataSpec):
    """Deploy cloud run jobs to materialise the dataset."""


@op(ins={"df_revisit_slice": In(DagsterS2IndexDF)}, out=Out())
def op_materialize_tile(df_revisit_slice: S2IndexDF, config: DataSpec):
    """Deploy cloud run jobs to materialise the dataset."""


@graph
def materialize_dataset():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    revisits.map(op_materialize_tile_run_job)


@graph
def materialize_dataset_local():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    revisits.map(op_materialize_tile)


materialize_local = materialize_dataset_local.to_job(
    name="materialize_dataset_locally",
    description="Materialize dataset locally for development and testing",
)
