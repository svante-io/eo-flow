from dagster import DynamicOut, DynamicOutput, In, Out, graph, op

from eoflow.models import DataSpec, Tile
from eoflow.tiles.catalogue import get_tiles


@op(out=DynamicOut())
def dynamic_tiles(config: DataSpec):
    tiles = get_tiles(config)
    for tile in tiles:
        yield DynamicOutput(tile, mapping_key=tile.tile)


@op(ins={"tile": In(Tile)}, out=Out())
def materialise_tile(tile: Tile, config: DataSpec):
    # get slice
    # get data
    # composite etc.
    pass


@graph
def materialise_dataset():
    tiles = dynamic_tiles()
    tiles.map(materialise_tile)


materialise_local = materialise_dataset.to_job(
    name="materialise_dataset_locally",
    description="Materialise dataset locally for development and testing",
)
