import os
from io import BytesIO
from typing import List

import dask.array as da
import numpy as np
from PIL import Image

from eoflow.lib.gcp import download_blob
from eoflow.lib.resize import imresize
from eoflow.models.models import (
    S2_BAND_RESOLUTION,
    S2_BUCKET,
    S2BandsEnum,
    UpsampleEnum,
)


def make_band_urls(mgrs_tile, product_id, granule_id):
    """return the band prefixes for the given product_id."""

    band_urls = {}
    for band in S2BandsEnum:
        band_urls[band.title()] = os.path.join(
            "L2",
            "tiles",
            mgrs_tile[0:2],
            mgrs_tile[2],
            mgrs_tile[3:5],
            product_id + ".SAFE",
            "GRANULE",
            granule_id,
            "IMG_DATA",
            "R" + S2_BAND_RESOLUTION[band.title()],
            "_".join(
                [
                    "T" + mgrs_tile,
                    product_id.split("_")[2],
                    band.title(),
                    S2_BAND_RESOLUTION[band.title()] + ".jp2",
                ]
            ),
        )
    return band_urls


class GCPS2Granule:

    def __init__(
        self,
        mgrs_tile: str,
        granule_id: str,
        product_id: str,
        bands: List[S2BandsEnum],
        upsample: UpsampleEnum,
    ):

        self.url = os.path.join(
            "L2",
            "tiles",
            mgrs_tile[0:2],
            mgrs_tile[2],
            mgrs_tile[3:5],
            product_id + ".SAFE",
            "GRANULE",
            granule_id,
        )
        self.bands = bands
        self.upsample = upsample
        self.band_urls = make_band_urls(mgrs_tile, product_id, granule_id)

        self._build_stack()

    def _build_stack(self):

        def read_one_band(block_id):
            """read a single band from the granule."""

            AXIS = 0

            band_url = self.band_urls[self.bands[block_id[AXIS]]]

            buffer = download_blob(S2_BUCKET, band_url)

            arr = np.array(Image.open(BytesIO(buffer)))

            if S2_BAND_RESOLUTION[self.bands[block_id[AXIS]]] == "20m":
                arr = imresize(arr, 2, kernel=self.upsample)
            if S2_BAND_RESOLUTION[self.bands[block_id[AXIS]]] == "60m":
                arr = imresize(arr, 6, kernel=self.upsample)

            return np.expand_dims(arr, AXIS)

        self.stack = da.map_blocks(
            read_one_band,
            dtype=np.uint16,
            chunks=((1,) * len(self.bands), 10980, 10980),
        )
