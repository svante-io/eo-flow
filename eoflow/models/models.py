from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Optional, TypeVar, Union

import pandas as pd
import pandera as pa
from dagster import Config
from dagster_pandera import pandera_schema_to_dagster_type
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from mgrs import MGRS
from pandera.typing import Series
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    conlist,
    field_validator,
)
from pyproj import CRS, Transformer
from shapely import geometry as shapely_geometry
from shapely.ops import transform

Point = tuple[float, float]
LinearRing = conlist(Point, min_length=4)
PolygonCoords = conlist(LinearRing, min_length=1)
MultiPolygonCoords = conlist(PolygonCoords, min_length=1)
BBox = tuple[float, float, float, float]  # 2D bbox
Props = TypeVar("Props", bound=dict)
VALID_GEOM_TYPES = [
    "Polygon",
    "Point",
    "LineString",
    "MultiPolygon",
    "MultiPoint",
    "MultiLineString",
]


class Geometry(BaseModel):
    type: str = Field(...)
    coordinates: Union[PolygonCoords, MultiPolygonCoords, Point] = Field(...)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"type": "Polygon"},
                {"coordinates": [[[1, 3], [2, 2], [4, 4], [1, 3]]]},
            ]
        }
    )

    def get(self, attr):
        return getattr(self, attr)

    def to_shapely(self):
        return shapely_geometry.shape(self.model_dump())


class CloudMaskEnum(str, Enum):
    S2CLOUDLESS = "S2CLOUDLESS"
    S2QUALITYMASK = "S2QUALITYMASK"


class CompositeEnum(str, Enum):
    FIRST = "FIRST"  # oldest
    LAST = "LAST"  # most recent
    MEAN = "MEAN"  # mean-pixel
    MAX = "MAX"  # brightest-pixel
    SCENEWISE_MAX = "SCENEWISE_MAX"  # brightest-scene
    SEQUENCE = "SEQUENCE"  # all scenes


class S2BandsEnum(str, Enum):
    B01 = "B01"
    B02 = "B02"
    B03 = "B03"
    B04 = "B04"
    B05 = "B05"
    B06 = "B06"
    B07 = "B07"
    B08 = "B08"
    B8A = "B8A"
    B09 = "B09"
    B10 = "B10"
    B11 = "B11"
    B12 = "B12"


S2_BAND_RESOLUTION = {
    "B02": "10m",
    "B03": "10m",
    "B04": "10m",
    "B08": "10m",
    "B05": "20m",
    "B06": "20m",
    "B07": "20m",
    "B8A": "20m",
    "B11": "20m",
    "B12": "20m",
    "B01": "60m",
    "B09": "60m",
    "B10": "60m",
}

S2_BUCKET = "gcp-public-data-sentinel-2"


class ConstellationEnum(str, Enum):
    S2 = "S2"


class UpsampleEnum(str, Enum):
    NEAREST = "nearest"
    BILINEAR = "bilinear"
    BICUBIC = "bicubic"
    LANCZOS = "lanczos"


class ThumbnailProps(Config):
    pixels: int = 256
    bands: list[S2BandsEnum] = ["B02", "B03", "B04"]

    # validate len(bands) in [1,3]


class LoaderOuputEnum(str, Enum):
    DICT = "dict"
    NDARRAY = "nd.array"
    XARRAY = "xarray"


class DataSpec(Config):
    target_geofile: str
    aoi_geofile: Optional[str]
    dataset_store: str
    start_datetime: Optional[str] = Field(
        ..., example=(datetime.now() - relativedelta(months=1)).isoformat()[0:10]
    )
    end_datetime: Optional[str] = Field(..., example=datetime.now().isoformat()[0:10])
    cloud_mask: Optional[list[CloudMaskEnum]] = None
    composite: CompositeEnum = CompositeEnum.LAST
    constellation: ConstellationEnum = ConstellationEnum.S2
    bands: list[S2BandsEnum] = ["B02", "B03", "B04"]
    chipsize: int = 256
    loader_output: LoaderOuputEnum = LoaderOuputEnum.NDARRAY
    upsample: Optional[UpsampleEnum] = UpsampleEnum.BILINEAR
    thumbnail: Optional[ThumbnailProps] = None
    clip: conlist(int, min_length=2, max_length=2) = [0, 4000]
    rescale: conlist(float, min_length=2, max_length=2) = [0, 1]

    @field_validator("upsample")
    def upsample_none(cls, v, values):
        # validate upsample can't be none if np.ndarray or xarray
        if (
            values.data["loader_output"]
            in [LoaderOuputEnum.NDARRAY, LoaderOuputEnum.XARRAY]
            and v is None
        ):
            raise ValueError(
                "upsample must be specified if loader_output is np.ndarray or xarray"
            )
        return v

    @field_validator("start_datetime")
    def start_datetime_default_1m(cls, v, values):
        if v is None:
            # default start_datetime to 1 month ago
            return (datetime.now() - relativedelta(months=1)).isoformat()[0:10]
        else:
            # validate input is parseable
            parse(v)
            return v

    @field_validator("end_datetime")
    def end_datetime_default_now(cls, v, values):
        if v is None:
            # default end_datetime to now
            return datetime.now().isoformat()[0:10]
        else:
            # validate input is parseable
            parse(v)
            return v


class Tile(BaseModel):
    tile: str

    @computed_field(return_type=Geometry)
    @cached_property
    def geometry_utm(self):
        utm_tile = MGRS().MGRSToUTM(self.tile)
        return Geometry(
            type="Polygon",
            coordinates=[
                [
                    [utm_tile[2], utm_tile[3] + 100020],
                    [utm_tile[2] + 109800, utm_tile[3] + 100020],
                    [utm_tile[2] + 109800, utm_tile[3] - 9780],
                    [utm_tile[2], utm_tile[3] - 9780],
                    [utm_tile[2], utm_tile[3] + 100020],
                ]
            ],
        )

    @computed_field(return_type=Geometry)
    @cached_property
    def geometry(self):
        utm_crs = CRS(self.utm_crs)
        wgs_crs = CRS("EPSG:4326")

        reproject = Transformer.from_crs(utm_crs, wgs_crs, always_xy=True).transform
        return Geometry(
            **shapely_geometry.mapping(
                transform(reproject, self.geometry_utm.to_shapely())
            )
        )

    @computed_field(return_type=str)
    @cached_property
    def utm_crs(self):
        utm_tile = MGRS().MGRSToUTM(self.tile)
        if utm_tile[1] == "N":
            return "EPSG:326" + str(utm_tile[0])
        else:  # S
            return "EPSG:327" + str(utm_tile[0])

    @computed_field(return_type=str)
    @cached_property
    def utm_crs_raw(self):
        if "326" in self.utm_crs:
            return "N" + self.utm_crs[-2:]
        else:
            return "S" + self.utm_crs[-2:]

    @computed_field(return_type=Geometry)
    @cached_property
    def utm_top_left(self):
        utm_tile = MGRS().MGRSToUTM(self.tile)
        return Geometry(type="Point", coordinates=[utm_tile[2], utm_tile[3] - 980])


class S2IndexItem(BaseModel):
    """Single granule index in BigQuery."""

    granule_id: str
    product_id: str
    datatake_identifier: str
    mgrs_tile: str
    sensing_time: datetime
    base_url: Optional[str]
    source_url: Optional[str]
    total_size: int
    cloud_cover: float

    @field_validator("sensing_time", mode="before")
    def parse_sensing_time(cls, v):
        if isinstance(v, str):
            return parse(v)
        return v


class S2IndexDF(pa.DataFrameModel):
    """Granule index in BigQuery."""

    granule_id: Series[str] = pa.Field(description="ID of the image")
    product_id: Series[str] = pa.Field(description="ID of the sensor and product")
    datatake_identifier: Series[str] = pa.Field(description="ID of the datatake")
    mgrs_tile: Series[str] = pa.Field(description="key for the UTM MGRS tile")
    sensing_time: Series[pd.DatetimeTZDtype(unit="us", tz="utc")] = (  # noqa: F821
        pa.Field(description="timestamp of the image")
    )
    base_url: Series[str] = pa.Field(
        description="GCS url of the granule", nullable=True
    )
    source_url: Series[str] = pa.Field(
        description="GCS URL of the granule source", nullable=True
    )
    total_size: Series[int] = pa.Field(description="Total size in bytes of the granule")
    cloud_cover: Series[float] = pa.Field(
        description="Cloud cover percentage out of 100"
    )

    # additional columns:
    # geometric_quality_flag
    # generation_time
    # north_lat
    # south_lat
    # west_lon
    # east_lon
    # source_url
    # etl_timestamp


def S2IndexDFtoItems(df: S2IndexDF):
    return [S2IndexItem(**item) for item in df.to_dict(orient="records")]


DagsterS2IndexDF = pandera_schema_to_dagster_type(S2IndexDF)
