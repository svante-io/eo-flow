from eoflow.models.archive import Archive
from eoflow.models.models import DataSpec, S2IndexItem, Tile


def write_materialized_index(tiles_indices: list[Tile]):
    pass


def materialize_tile(tile: Tile, revisits: list[S2IndexItem], config: DataSpec):
    """Materialize (i.e. fetch data; mask; composite; and store) a single tile of the dataspec."""

    archive = Archive(cfg=config, tile=tile, revisits=revisits)

    # retrieve granules
    archive.fill()

    # apply masks
    archive.mask()

    # composite and materialize, returning the index
    return archive.materialize()
