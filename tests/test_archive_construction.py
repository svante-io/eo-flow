import pytest
import zarr

from eoflow.models import Archive


@pytest.mark.order(1)
def test_archive_construction(
    sample_dataspec, sample_archive_tile, sample_archive_revisits
):
    """
    Test the archive construction from Tile and list[S2IndexItem] (i.e. including pulling data)
    Monkeypatch local storage of data.
    """

    archive = Archive(
        cfg=sample_dataspec,
        tile=sample_archive_tile,
        revisits=sample_archive_revisits,
    )

    archive.fill()

    assert True


@pytest.mark.order(2)
def test_archive_mask_and_composite(
    sample_dataspec, sample_archive_tile, sample_archive_revisits
):
    """
    Test the archive mask and composite operations,
    """

    archive = Archive(
        cfg=sample_dataspec,
        tile=sample_archive_tile,
        revisits=sample_archive_revisits,
    )

    # pseudo-monkey patch, assume the existance of the archive from the test above.
    archive.z = zarr.open("./local-30UXC.zarr", "r+")

    archive.mask()

    archive.composite()

    assert True


@pytest.mark.order(3)
def test_archive_store_chips_and_targets(
    sample_dataspec, sample_archive_tile, sample_archive_revisits
):
    """
    Test the archive mask and composite operations,
    """

    archive = Archive(
        cfg=sample_dataspec,
        tile=sample_archive_tile,
        revisits=sample_archive_revisits,
    )

    # pseudo-monkey patch, assume the existance of the archive from the test above.
    archive.z = zarr.open("./local-30UXC.zarr", "r+")

    archive.mask()

    archive.materialize()

    assert True
