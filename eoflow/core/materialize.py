import time

from eoflow.core.logging import logger as local_logger
from eoflow.models.archive import Archive
from eoflow.models.models import DataSpec, S2IndexItem, Tile


def write_materialized_index(tiles_indices: list[Tile]):
    pass


def materialize_tile(
    tile: Tile, revisits: list[S2IndexItem], config: DataSpec, logger=local_logger
):
    """Materialize (i.e. fetch data; mask; composite; and store) a single tile of the dataspec."""

    tic = time.time()

    logger.info(f"{tile.tile}:{time.time() - tic:.2f} Materializing...")
    archive = Archive(cfg=config, tile=tile, revisits=revisits)

    logger.info(
        f"{tile.tile}:{time.time() - tic:.2f} Built Archive, {len(archive.chips)} chips"
    )
    archive.fill()

    logger.info(
        f"{tile.tile}:{time.time() - tic:.2f} Filled Archive, shape: {archive.z.shape}"
    )
    archive.mask()

    logger.info(f"{tile.tile}:{time.time() - tic:.2f} Built Mask")

    # composite and materialize, returning the index
    idx = archive.materialize()
    logger.info(f"{tile.tile}:{time.time() - tic:.2f} Materialized Archived!")
    return idx
