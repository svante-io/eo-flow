import shapely
from shapely import wkt

from eoflow.models import Tile


def test_tile_construction():
    tile = Tile(
        tile="30UXC",
    )

    # WGS tile geometry
    # "POLYGON ((-1.5321158471 52.34135509680001, 0.0777579253 52.3103669144, 0.0112541226 51.3245233542, -1.5638793749 51.35443938980001, -1.5321158471 52.34135509680001))"

    TILE_GEOMETRY_UTM = wkt.loads(
        "POLYGON ((600000. 5800020, 709800. 5800020., 709800. 5690220, 600000. 5690220., 600000 5800020.))"
    )

    assert tile.tile == "30UXC"
    assert tile.utm_crs == "EPSG:32630"
    assert shapely.equals(tile.geometry.to_shapely(), TILE_GEOMETRY_UTM)
