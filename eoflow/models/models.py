from datetime import datetime
from enum import Enum
from typing import Optional, TypeVar, Union

from dagster import Config
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, conlist, validator

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
    type: str = Field(..., example="Polygon")
    coordinates: Union[PolygonCoords, MultiPolygonCoords, Point] = Field(
        ..., example=[[[1, 3], [2, 2], [4, 4], [1, 3]]]
    )

    def get(self, attr):
        return getattr(self, attr)


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

    @validator("upsample")
    def upsample_none(cls, v, values):
        # validate upsample can't be none if np.ndarray or xarray
        if (
            values["loader_output"] in [LoaderOuputEnum.NDARRAY, LoaderOuputEnum.XARRAY]
            and v is None
        ):
            raise ValueError(
                "upsample must be specified if loader_output is np.ndarray or xarray"
            )
        return v

    @validator("start_datetime")
    def start_datetime_default_1m(cls, v, values):
        if v is None:
            # default start_datetime to 1 month ago
            return (datetime.now() - relativedelta(months=1)).isoformat()[0:10]
        else:
            # validate input is parseable
            parse(v)
            return v

    @validator("end_datetime")
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
    geometry: Optional[Geometry]
