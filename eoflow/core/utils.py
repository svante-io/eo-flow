import fiona
import geopandas as gpd
from cloudpathlib import AnyPath, CloudPath


def read_any_geofile(path: str) -> gpd.GeoDataFrame:

    input_file = AnyPath(path)

    if isinstance(input_file, CloudPath):

        if input_file.stat().st_size / 1e6 > 100:
            raise ValueError("Cloud geofile size exceeds maximum allowable: 100mb.")

        # is cloudpath
        with open(input_file, "rb") as f:
            buffer = f.read()

        with fiona.BytesCollection(buffer) as f:
            crs = f.crs
            return gpd.GeoDataFrame.from_features(f, crs=crs)

    else:
        # is local path
        return gpd.read_file(str(input_file))
