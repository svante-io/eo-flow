import json

from cloudpathlib import AnyPath
from dagster import DynamicOut, DynamicOutput, In, Out, graph, op
from dagster_pandera import pandera_schema_to_dagster_type

from eoflow.core.catalogue import get_revisits, get_tiles
from eoflow.core.materialize import materialize_tile
from eoflow.models import Archive, ArchiveIndex, DataSpec, S2IndexDF, Tile

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


@op(ins={"df_revisit_slice": In(DagsterS2IndexDF)}, out=Out(ArchiveIndex))
def op_materialize_tile(df_revisit_slice: S2IndexDF, config: DataSpec):
    """Deploy cloud run jobs to materialise the dataset."""

    revisits = df_revisit_slice.to_pydantic()
    tile = Tile(tile=df_revisit_slice["mgrs_tile"].values[0])

    return materialize_tile(tile=tile, revisits=revisits, config=config)


@op(ins={"archive_indices": In(list[ArchiveIndex])}, out=Out())
def op_merge_and_store_dataset_index(
    archive_indices: list[ArchiveIndex], config: DataSpec
):
    merged_index = Archive.merge_archive_indices(archive_indices)
    json.dump(
        merged_index.model_dump_json(), open(AnyPath(config.store + "/index.json"), "w")
    )


@graph
def materialize_dataset():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    revisits.map(op_materialize_tile_run_job)


@graph
def materialize_dataset_local():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    archive_indices = revisits.map(op_materialize_tile)
    op_merge_and_store_dataset_index(archive_indices.collect())


materialize_local = materialize_dataset_local.to_job(
    name="materialize_dataset_locally",
    description="Materialize dataset locally for development and testing",
)
