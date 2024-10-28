from eoflow.models.archive import Archive
from eoflow.models.models import DataSpec, S2IndexItem, Tile


def write_materialized_index(tiles_indices: list[Tile]):
    pass


def materialize_tile(revisits: list[S2IndexItem], config: DataSpec):
    """Materialize (i.e. fetch data; mask; composite; and store) a single tile of the dataspec."""

    archive = Archive(path=config.archive_path, cfg=config, revisits=revisits)

    # retrieve granules
    archive.fill()

    # apply masks
    archive.mask()

    # composite
    archive.composite()
