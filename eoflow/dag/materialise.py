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
def materialise_tile(df_revisit_slice: S2IndexDF, config: DataSpec):
    # do the fun stuff here
    # create archives {resolution: {revisit,band, x, y}}
    # retreive granules, upsampling
    # apply masks (cloud, target/aoi) and compositing
    # create chips and write to storage
    # create targets/masks and write to storage
    # create any other artefact and metadata
    pass


@graph
def materialise_dataset():
    tiles = get_tiles_op()
    revisits = dynamic_revisits(tiles)
    revisits.map(materialise_tile)


materialise_local = materialise_dataset.to_job(
    name="materialise_dataset_locally",
    description="Materialise dataset locally for development and testing",
)
