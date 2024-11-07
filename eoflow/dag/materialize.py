import json

import google.cloud.storage
from cloudpathlib import AnyPath
from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    InitResourceContext,
    OpExecutionContext,
    Out,
    graph,
    op,
    resource,
)

from eoflow.cloud.materialize import (
    PipesCloudStorageMessageReader,
    PipesEagerJobClient,
    op_materialize_tile_eager,
)
from eoflow.core.catalogue import get_revisits, get_tiles
from eoflow.core.materialize import materialize_tile
from eoflow.models import (
    Archive,
    ArchiveIndex,
    DagsterS2IndexDF,
    DataSpec,
    S2IndexDF,
    S2IndexDFtoItems,
    Tile,
)


@op(out=Out())
def get_tiles_op(context: OpExecutionContext, config: DataSpec):
    return get_tiles(config, logger=context.log)


@op(ins={"tiles": In(list[Tile])}, out=Out(dagster_type=DagsterS2IndexDF))
def op_revisits(context: OpExecutionContext, tiles: list[Tile], config: DataSpec):
    if len(tiles) > 10:
        context.log.info(f"got {len(tiles)} tiles")
    else:
        context.log.info("Got tiles: {}".format(", ".join(t.tile for t in tiles)))

    df_revisits = get_revisits(tiles, config)

    context.log.info(f"Got {len(df_revisits)} revisits for {len(tiles)} tiles")

    return df_revisits


@op(ins={"tiles": In(list[Tile])}, out=DynamicOut(dagster_type=DagsterS2IndexDF))
def dynamic_revisits(context: OpExecutionContext, tiles: list[Tile], config: DataSpec):
    if len(tiles) > 10:
        context.log.info(f"got {len(tiles)} tiles")
    else:
        context.log.info("Got tiles: {}".format(", ".join(t.tile for t in tiles)))

    df_revisits = get_revisits(tiles, config)

    context.log.info(f"Got {len(df_revisits)} revisits for {len(tiles)} tiles")

    for mgrs_tile, df_revisit_slice in df_revisits.groupby("mgrs_tile"):
        yield DynamicOutput(df_revisit_slice, mapping_key=mgrs_tile)


@op(ins={"df_revisit_slice": In(DagsterS2IndexDF)}, out=Out(ArchiveIndex))
def op_materialize_tile_local(
    context: OpExecutionContext, df_revisit_slice: S2IndexDF, config: DataSpec
):
    """Deploy local materialisation of the tile."""

    revisits = S2IndexDFtoItems(df_revisit_slice)
    tile = Tile(tile=df_revisit_slice["mgrs_tile"].values[0])

    return materialize_tile(
        tile=tile, revisits=revisits, config=config, logger=context.log
    )


@op(ins={"archive_indices": In(list[ArchiveIndex])}, out=Out())
def op_merge_and_store_dataset_index(
    archive_indices: list[ArchiveIndex], config: DataSpec
):
    merged_index = Archive.merge_archive_indices(archive_indices)
    json.dump(
        json.loads(merged_index.model_dump_json()),
        open(AnyPath(config.dataset_store + "/index.json"), "w"),
    )
    json.dump(
        json.loads(config.model_dump_json()),
        open(AnyPath(config.dataset_store + "/dataspec.json"), "w"),
    )


@graph
def materialize_dataset_eager():
    tiles = get_tiles_op()
    revisits = op_revisits(tiles)
    archive_indices = op_materialize_tile_eager(revisits)
    op_merge_and_store_dataset_index(archive_indices)


@graph
def materialize_dataset_local():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    archive_indices = revisits.map(op_materialize_tile_local)
    op_merge_and_store_dataset_index(archive_indices.collect())


materialize_local = materialize_dataset_local.to_job(
    name="materialize_dataset_locally",
    description="Materialize dataset locally for development and testing",
)


@resource
def pipes_run_job_client(context: InitResourceContext) -> PipesEagerJobClient:
    return PipesEagerJobClient(
        message_reader=PipesCloudStorageMessageReader(
            bucket="eo-flow-dev",
            prefix="initial_tests/" + context.run_id,
            client=google.cloud.storage.Client(),
        )
    )


materialize_eager = materialize_dataset_eager.to_job(
    name="materialize_dataset_eager",
    description="Materialize dataset eagerly for production",
    resource_defs={"pipes_run_job_client": pipes_run_job_client},
)
