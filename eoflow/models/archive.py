import dask.array as da
import geopandas as gpd
import numpy as np
from sentinelhub import CRS, UtmZoneSplitter

from eoflow.models.granule import GCPS2Granule
from eoflow.models.models import DataSpec, S2IndexItem, Tile


class Archive:

    def __init__(self, cfg: DataSpec, tile: Tile, revisits: list[S2IndexItem]):
        self.cfg = cfg
        self.tile = tile
        self.revisits = sorted(revisits, key=lambda x: x.datetime)

        # retrieve intersecting features
        gdf = gpd.read_file(cfg.target_geofile)
        self.gdf = gdf.loc[gdf.intersects(self.tile.geometry)]

        self._get_granules()
        self._create_data_stack()
        self._get_chips()

    def _get_granules(self):
        """instantiate all granules."""
        self.granules: list[GCPS2Granule] = [
            GCPS2Granule(
                mgrs_tile=self.tile.tile,
                granule_id=revisit.granule_id,
                product_id=revisit.product_id,
                bands=self.cfg.bands,
                upsample=self.cfg.upsample,
            )
            for revisit in self.revisits
        ]

    def _create_data_stack(self):
        """lazily create the archive for computation from the dask array."""

        self.data = da.concatenate([granule.stack for granule in self.granules], axis=0)

    def _get_chips(self):
        """create chips from the archive"""

        splitter = UtmZoneSplitter(
            self.gdf.unary_union, CRS.WGS84, (self.cfg.chipsize, self.cfg.chipsize)
        )

        bbox_list = splitter.get_bbox_list()
        info_list = splitter.get_info_list()

        chip_polygons = []
        for bbox, info in zip(bbox_list, info_list):
            if info.get("crs") != self.tile.crs:
                """only process chips in the same crs as the tile."""
                continue
            chip_polygons.append(bbox.geometry)

        chips = gpd.GeoDataFrame(geometry=chip_polygons, crs=self.tile.crs)
        chips["tile_minpx"] = (
            (chips.geometry.bounds.minx - self.tile.utm_top_left.x) / 10
        ).astype(int)
        chips["tile_maxpx"] = (
            (chips.geometry.bounds.maxx - (self.tile.utm_top_left.x - 10000)) / 10
        ).astype(int)
        chips["tile_minpy"] = (
            (chips.geometry.bounds.miny - self.tile.utm_top_left.y) / 10
        ).astype(int)
        chips["tile_maxpy"] = (
            (chips.geometry.bounds.maxy - (self.tile.utm_top_left.y - 10000)) / 10
        ).astype(int)
        self.chips = chips

    def fill(self):
        """execute the dask-delayed computation to mask, fill, and composite."""
        self.data.compute()

    def mask(self):
        """mask the archive data"""

        # 1. mask non-scope chips
        mask = np.ones(self.data.shape, dtype=bool)
        for _idx, chip in self.chips.iterrows():
            mask[
                :,
                chip["tile_minpx"] : chip["tile_maxpx"],  # noqa: E203
                chip["tile_minpy"] : chip["tile_maxpy"],  # noqa: E203
            ] = False

        # 2. mask non-data pixels
        mask = np.logical_or(mask, self.data == 0)

        # 3. mask clouds
        # TODO

        self.data = da.ma.masked_array(self.data, mask=self.mask)

    def composite(self):
        """composite the archive data"""

        def _composite_reduction(chunk_idx):
            pass

        self.data_composite = self.data.map_blocks()

    def iter_chips(self):
        pass
