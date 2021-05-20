import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import fiona
import os
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

WATERBODY_DBF = os.path.join(os.getenv("WATERBODY_DBF", "D:\\data\cyan_rare\\mounts\\geometry"), "waterbodies_4.dbf")


def get_waterbody(objectid: int = None, objectids: list = None, tojson: bool = False):
    features = []
    if tojson:
        with fiona.open(WATERBODY_DBF) as waterbodies:
            crs = waterbodies.crs
            for f in waterbodies:
                if objectid:
                    if objectid == f["properties"]["OBJECTID"]:
                        features.append(f)
                        break
                elif objectids:
                    if f["properties"]["OBJECTID"] in objectids:
                        features.append(f)
                        continue
                else:
                    features.append(f)
            geojson = []
            for feature in features:
                if feature["geometry"]["type"] == "MultiPolygon":
                    poly_geos = []
                    for p in feature["geometry"]["coordinates"]:
                        poly_geos.append(Polygon(p[0]))
                    poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
                else:
                    poly = gpd.GeoSeries(Polygon(feature["geometry"]["coordinates"][0]), crs=crs)
                geojson.append(poly.to_json())
            return geojson
    else:
        with fiona.open(WATERBODY_DBF) as waterbodies:
            crs = waterbodies.crs
            for f in waterbodies:
                if objectid:
                    if objectid == f["properties"]["OBJECTID"]:
                        features.append(f)
                        continue
                elif objectids:
                    if f["properties"]["OBJECTID"] in objectids:
                        features.append(f)
                        continue
                else:
                    features.append(f)
        return features, crs


def get_waterbody_byname(gnis_name: str):
    gnis_name = gnis_name.replace('\'', "").replace('\"', "")
    waterbody = []
    with fiona.open(WATERBODY_DBF) as waterbodies:
        for f in waterbodies:
            if gnis_name.lower() in f["properties"]["GNIS_NAME"].lower():
                wb = {
                    "name": f["properties"]["GNIS_NAME"],
                    "objectid": int(f["properties"]["OBJECTID"]),
                    "centroid_lat": float(f["properties"]["c_lat"]),
                    "centroid_lng": float(f["properties"]["c_lng"]),
                }
                waterbody.append(wb)
    return waterbody


def get_waterbody_properties(objectid: int):
    metadata = {}
    with fiona.open(WATERBODY_DBF) as waterbodies:
        for f in waterbodies:
            if objectid == int(f["properties"]["OBJECTID"]):
                metadata = f["properties"]
                break
    return metadata
