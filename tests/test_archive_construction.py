import pytest

from eoflow.models import Archive


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


@pytest.mark.skip(reason="expensive test, only run when necessary")
def test_archive_mask_and_composite():
    """
    Test the archive mask and composite operations, monkeypatch loading the data stack
    """
