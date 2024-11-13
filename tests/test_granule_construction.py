import dask
import zarr

from eoflow.models.granule import GCPS2Granule


def test_granule_construction(
    sample_dataspec, sample_archive_tile, sample_archive_revisits
):
    """
    Test the archive construction from Tile and list[S2IndexItem] (i.e. including pulling data)
    Monkeypatch local storage of data.
    """

    granule = GCPS2Granule(
        mgrs_tile=sample_archive_tile.tile,
        granule_id=sample_archive_revisits[0].granule_id,
        product_id=sample_archive_revisits[0].product_id,
        bands=sample_dataspec.bands,
        upsample=sample_dataspec.upsample,
    )

    z = zarr.open(
        "./test_granule_construction.zarr",
        mode="w",
        shape=(3, len(sample_dataspec.bands), 10980, 10980),
        chunks=(1, 1, sample_dataspec.chipsize, sample_dataspec.chipsize),
        dtype="uint16",
    )

    job = granule.stack.store(
        z,
        regions=(0, slice(None), slice(None), slice(None)),
        compute=False,
        return_stored=False,
    )

    dask.compute(job)

    assert True
