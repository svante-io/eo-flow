from itertools import chain
from typing import Optional, Union

import dask
import dask.array as da
import geopandas as gpd
import numpy as np
import zarr
from cloudpathlib import AnyPath
from pydantic import BaseModel, field_validator
from rasterio import Affine, features
from sentinelhub import CRS, UtmZoneSplitter
from xarray import DataArray as xda

from eoflow.core.utils import read_any_geofile
from eoflow.models.granule import GCPS2Granule
from eoflow.models.models import DataSpec, S2IndexItem, Tile


class ChipStats(BaseModel):
    mean: list[Union[float, None]]
    std: list[Union[float, None]]

    @field_validator("*")
    @classmethod
    def none2nan(cls, v):
        if None in v:
            return np.array(v).astype(float).tolist()
        return v


class Indexbase(BaseModel):
    tile: str
    chip_ii: int
    chip_idx: str


class ChipIndex(Indexbase):
    chip_path: str
    chip_stats: ChipStats


class TargetIndex(Indexbase):
    target_path: str
    target_pxcount: dict[int, int]


class ChipMetaData(ChipIndex, TargetIndex):
    pass


class ArchiveIndex(BaseModel):
    tile: str
    chips: list[ChipMetaData]


class DataSetIndex(BaseModel):
    chips: list[ChipMetaData]
    chip_stats: ChipStats


class Archive:

    def __init__(
        self,
        cfg: DataSpec,
        tile: Tile,
        revisits: list[S2IndexItem],
        run_id: Optional[str] = None,
    ):
        self.cfg = cfg
        self.tile = tile
        self.revisits = sorted(
            revisits, key=lambda x: x.sensing_time
        )  # most recent last

        if run_id is not None:
            self.store = f"{cfg.dataset_store}/{run_id}"
        else:
            self.store = cfg.dataset_store

        # retrieve intersecting features
        gdf = read_any_geofile(cfg.target_geofile)
        self.gdf = gdf.loc[gdf.intersects(self.tile.geometry.to_shapely())]

        self._get_granules()
        self._create_lazy_data_store()
        self._get_chips()

        # don't need wgs gdf anymore, cast it to tile_crs
        self.gdf = self.gdf.to_crs(self.tile.utm_crs)

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

    def _create_lazy_data_store(self):
        """lazily create the archive for computation from the dask array."""

        self.stack = da.stack(
            [granule.stack for granule in self.granules],
            axis=0,
        )

        return True

    def _get_chips(self):
        """create chips from the archive"""

        splitter = UtmZoneSplitter(
            [self.gdf.union_all()], CRS.WGS84, (self.cfg.chipsize, self.cfg.chipsize)
        )

        bbox_list = splitter.get_bbox_list()

        chip_polygons = []
        transforms = []
        for bbox in bbox_list:
            if str(bbox.crs) != self.tile.utm_crs:
                """only process chips in the same crs as the tile."""
                continue
            chip_polygons.append(bbox.geometry)
            transforms.append(bbox.get_transform_vector(10, 10))

        chips = gpd.GeoDataFrame(geometry=chip_polygons, crs=self.tile.utm_crs)
        chips["tile_minpx"] = (
            (chips.geometry.bounds.minx - self.tile.utm_top_left.to_shapely().x) / 10
        ).astype(int)
        chips["tile_maxpx"] = (
            (
                chips.geometry.bounds.maxx
                - (self.tile.utm_top_left.to_shapely().x - 10000)
            )
            / 10
        ).astype(int)
        chips["tile_minpy"] = (
            (chips.geometry.bounds.miny - self.tile.utm_top_left.to_shapely().y) / 10
        ).astype(int)
        chips["tile_maxpy"] = (
            (
                chips.geometry.bounds.maxy
                - (self.tile.utm_top_left.to_shapely().y - 10000)
            )
            / 10
        ).astype(int)
        chips["affine_transform"] = transforms

        self.chips = chips

    def fill(self):
        """execute the dask-delayed computation to mask, fill, and composite."""

        self.z = zarr.open(
            f"./local-{self.tile.tile}.zarr",
            mode="w",
            shape=(len(self.revisits), len(self.cfg.bands), 10980, 10980),
            chunks=(1, 1, self.cfg.chipsize, self.cfg.chipsize),
            dtype="uint16",
        )

        stack = da.stack([g.stack for g in self.granules], axis=0)

        job = stack.store(self.z, compute=False, return_stored=False)

        dask.compute(job, num_workers=2)

    def _generate_mask(self):
        """mask the archive data"""

        shape = (len(self.revisits), 10980, 10980)
        # 1. mask non-scope chips
        mask = np.ones(shape, dtype=bool)
        for _idx, chip in self.chips.iterrows():
            mask[
                :,
                chip["tile_minpx"] : chip["tile_maxpx"],  # noqa: E203
                chip["tile_minpy"] : chip["tile_maxpy"],  # noqa: E203
            ] = False

        # 2. mask non-data pixels
        non_data_mask = np.zeros(shape, dtype=bool)

        # 2a. there's a small number of chips, just map using chips
        for _idx, chip in self.chips.iterrows():
            non_data_mask[
                :,
                chip.tile_minpx : chip.tile_maxpx,  # noqa: E203
                chip.tile_minpy : chip.tile_maxpy,  # noqa: E203
            ] = (
                self.z[
                    :,
                    :,
                    chip.tile_minpx : chip.tile_maxpx,  # noqa: E203
                    chip.tile_minpy : chip.tile_maxpy,  # noqa: E203
                ]
                == 0
            ).all(
                axis=1
            )

        # 2b. there's a large number of chips, map the whole dataset using chunks
        self.mask = np.logical_or(mask, non_data_mask)
        del non_data_mask

        # 3. mask clouds
        # TODO

    def mask(self):
        """mask the archive data"""

        self._generate_mask()

    def _composite_chip(
        self,
        block_id: Optional[tuple[int]] = None,
        chip: Optional[gpd.GeoSeries] = None,
    ):

        def composite_first(arr):
            """composite using the most-recent non-nan value"""

            arr = (
                xda(
                    arr,
                    dims=("R", "B", "X", "Y"),
                )
                .ffill(dim="R")
                .to_numpy()
                .astype(np.uint16)
            )

            return arr[0, :, :, :]

        def composite_last(arr):
            return composite_first(arr[:, ::-1, :, :])

        if chip is not None:
            x_slice = slice(chip.tile_minpx, chip.tile_maxpx)
            y_slice = slice(chip.tile_minpy, chip.tile_maxpy)
        elif block_id:
            x_slice = slice(
                block_id[2] * self.cfg.chipsize, (block_id[2] + 1) * self.cfg.chipsize
            )
            y_slice = slice(
                block_id[3] * self.cfg.chipsize, (block_id[3] + 1) * self.cfg.chipsize
            )
        else:
            raise ValueError("neither chip nor block_id is provided.")

        if self.mask[:, x_slice, y_slice].all():  # R, X, Y
            """maybe shortcut if all data is masked"""
            return (
                np.ones(
                    (len(self.cfg.bands), self.cfg.chipsize, self.cfg.chipsize),
                    dtype=self.z.dtype,
                )
                * np.nan
            )

        arr = self.z[
            :,
            :,
            chip.tile_minpx : chip.tile_maxpx,  # noqa: E203
            chip.tile_minpy : chip.tile_maxpy,  # noqa: E203
        ]

        if self.cfg.composite == "FIRST":
            return composite_first(arr)
        elif self.cfg.composite == "LAST":
            return composite_last(arr)
        else:
            raise NotImplementedError("Only FIRST and LAST composite is supported.")

    def _burn_target(self, chip):
        """burn the target data into an image matching the chip"""

        transform = Affine.from_gdal(*chip.affine_transform)

        return np.array(
            features.rasterize(
                [
                    (target.geometry, 255)
                    for _idx, target in self.gdf.loc[
                        self.gdf.intersects(chip.geometry)
                    ].iterrows()
                ],
                out_shape=(self.cfg.chipsize, self.cfg.chipsize),
                transform=transform,
                fill=0,
                all_touched=False,
                dtype="uint8",
            )
        )

    def composite(self):
        """composite the whole archive data"""

        return da.map_blocks(
            self._composite_chip,
            chunks=(
                len(self.revisits),
                len(self.cfg.bands),
                self.cfg.chipsize,
                self.cfg.chipsize,
            ),
            dtype=self.z.dtype,
        ).compute()

    def composite_chips(self):
        """iterate over chips and composite"""
        for _idx, chip in self.chips.iterrows():
            yield self._composite_chip(chip=chip)

    def _prep_chip_path(self):
        """prepare the chip path"""
        root_pth = AnyPath(f"{self.store}/chips/")
        root_pth.mkdir(parents=True, exist_ok=True)

    def _prep_target_path(self):
        """prepare the target path"""
        root_pth = AnyPath(f"{self.store}/targets/")
        root_pth.mkdir(parents=True, exist_ok=True)

    def prep_archive_paths(self):
        """prepare the archive paths"""
        self._prep_chip_path()
        self._prep_target_path()

    def _store_chip(self, ii: int, chip):
        """store the composite chip"""
        chip_data = self._composite_chip(chip=chip)
        pth = AnyPath(f"{self.store}/chips/{self.tile.tile}-{ii}.npy")
        pth.write_bytes(chip_data.tobytes())
        return ChipIndex(
            tile=self.tile.tile,
            chip_ii=ii,
            chip_idx=self.tile.tile + f"-{ii}",
            chip_path=str(pth),
            chip_stats={
                "mean": np.nanmean(chip_data, axis=(1, 2)).tolist(),
                "std": np.nanstd(chip_data, axis=(1, 2)).tolist(),
            },
        )

    def _store_target(self, ii: int, chip):
        """store the target data"""
        target_img = self._burn_target(chip)
        pth = AnyPath(f"{self.store}/targets/{self.tile.tile}-{ii}.npy")
        pth.write_bytes(target_img.tobytes())
        val, counts = np.unique(target_img, return_counts=True)
        return TargetIndex(
            tile=self.tile.tile,
            chip_ii=ii,
            chip_idx=self.tile.tile + f"-{ii}",
            target_path=str(pth),
            target_pxcount=dict(zip(val.tolist(), counts.tolist())),
        )

    def store_chips_eager(self):
        """store the composite chips"""

        self._prep_chip_path()

        for ii, (_idx, chip) in enumerate(self.chips.iterrows()):
            self._store_chip(ii, chip)

    def store_targets_eager(self):
        """store the target data"""

        self._prep_target_path()

        for ii, (_idx, chip) in enumerate(self.chips.iterrows()):
            self._store_target(ii, chip)

    def store_chips(self):

        for ii, (_idx, chip) in enumerate(self.chips.iterrows()):
            yield dask.delayed(self._store_chip)(ii, chip)

    def store_targets(self):
        for ii, (_idx, chip) in enumerate(self.chips.iterrows()):
            yield dask.delayed(self._store_target)(ii, chip)

    def _merge_indices(
        self, chip_indices: list[ChipIndex], target_indices: list[TargetIndex]
    ) -> ArchiveIndex:

        target_index = {t.chip_idx: t.model_dump() for t in target_indices}
        chip_index = {c.chip_idx: c.model_dump() for c in chip_indices}

        # merge chip and target data together and cast to ChipMetaData
        chip_data = {k: {**chip_index[k], **target_index[k]} for k in chip_index.keys()}

        chip_data = [ChipMetaData(**chip_data[k]) for k in chip_data.keys()]

        return ArchiveIndex(tile=self.tile.tile, chips=chip_data)

    def materialize(self):
        """materialize the archive data"""

        self.prep_archive_paths()

        store_chip_futures = [fn for fn in self.store_chips()]
        store_target_futures = [fn for fn in self.store_targets()]

        chip_indices = dask.compute(*store_chip_futures, num_workers=4)  # fast!
        target_indices = dask.compute(*store_target_futures, num_workers=4)

        return self._merge_indices(chip_indices, target_indices)

    @classmethod
    def merge_archive_indices(cls, indices: list[ArchiveIndex]):
        return DataSetIndex(
            chips=list(chain.from_iterable([idx.chips for idx in indices])),
            chip_stats={
                "mean": np.nanmean(
                    np.array(
                        [
                            chip.chip_stats.mean
                            for chip in list(
                                chain.from_iterable([idx.chips for idx in indices])
                            )
                        ]
                    ),
                    axis=1,
                ).tolist(),
                "std": np.nanmean(
                    np.array(
                        [
                            chip.chip_stats.std
                            for chip in list(
                                chain.from_iterable([idx.chips for idx in indices])
                            )
                        ]
                    ),
                    axis=1,
                ),
            },
        )
