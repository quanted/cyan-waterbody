import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import fiona
import os
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

import time

WATERBODY_DBF = os.path.join(os.getenv("WATERBODY_DBF", "D:\\data\cyan_rare\\mounts\\geometry"), "waterbodies_9.dbf")
COUNTY_DBF = os.path.join(os.getenv("COUNTY_DBF", "D:\\data\cyan_rare\\mounts\\geometry"), "cb_2020_us_county_500k.dbf")
STATE_DBF = os.path.join(os.getenv("STATE_DBF", "D:\\data\cyan_rare\\mounts\\geometry"), "cb_2020_us_state_500k.dbf")
TRIBE_DBF = os.path.join(os.getenv("TRIBE_DBF", "D:\\data\cyan_rare\\mounts\\geometry"), "cb_2020_us_aiannh_500k.dbf")


def get_waterbody_fids(return_dict: bool = False):
    if return_dict:
        results = {}
    else:
        results = []
    with fiona.open(WATERBODY_DBF) as waterbodies:
        for f in waterbodies:
            if return_dict:
                results[int(f["properties"]["OBJECTID"])] = int(f["id"])
            else:
                results.append((int(f["properties"]["OBJECTID"]), int(f["id"])))
    return results


def get_waterbody_by_fids(fid: int = None, fids: list = None, tojson: bool = False):
    features = []
    if fid is None and fids is None:
        return features
    if tojson:
        with fiona.open(WATERBODY_DBF) as waterbodies:
            crs = waterbodies.crs
            if fid:
                f = waterbodies.get(fid)
                features.append(f)
            if fids:
                for _fid in fids:
                    f = waterbodies.get(_fid)
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
            if fid:
                f = waterbodies.get(fid)
                features.append(f)
            if fids:
                for _fid in fids:
                    f = waterbodies.get(_fid)
                    features.append(f)
        return features, crs


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
        i = 0
        with fiona.open(WATERBODY_DBF) as waterbodies:
            crs = waterbodies.crs
            for f in waterbodies:
                i += 1
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
        # print(f"ObjectID: {objectid}, index: {i-1}")
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
                    "areasqkm": float(f["properties"]["AREASQKM"]),
                    "state_abbr": f["properties"]["STATE_ABBR"]
                }
                waterbody.append(wb)
    return waterbody


def get_waterbody_byID(id: int, fid: int = None):
    if id is None and fid is None:
        return {}
    waterbody = []
    t0 = time.time()
    if not fid:
        with fiona.open(WATERBODY_DBF) as waterbodies:
            for f in waterbodies:
                if id == int(f["properties"]["OBJECTID"]):
                    wb = {
                        "name": f["properties"]["GNIS_NAME"],
                        "objectid": int(f["properties"]["OBJECTID"]),
                        "centroid_lat": float(f["properties"]["c_lat"]),
                        "centroid_lng": float(f["properties"]["c_lng"]),
                        "areasqkm": float(f["properties"]["AREASQKM"]),
                        "state_abbr": f["properties"]["STATE_ABBR"]
                    }
                    waterbody = [wb]
                    break
    else:
        with fiona.open(WATERBODY_DBF) as waterbodies:
            f = waterbodies.get(fid)
            if int(f["properties"]["OBJECTID"]) == id:
                wb = {
                    "name": f["properties"]["GNIS_NAME"],
                    "objectid": int(f["properties"]["OBJECTID"]),
                    "centroid_lat": float(f["properties"]["c_lat"]),
                    "centroid_lng": float(f["properties"]["c_lng"]),
                    "areasqkm": float(f["properties"]["AREASQKM"]),
                    "state_abbr": f["properties"]["STATE_ABBR"]
                }
                waterbody = [wb]
            else:
                return get_waterbody_byID(id=id)
    t1 = time.time()
    print(f"Search runtime: {round(t1-t0, 3)} sec")
    return waterbody


def get_waterbody_properties(objectid: int, fid: int = None):
    metadata = {}
    with fiona.open(WATERBODY_DBF) as waterbodies:
        if fid:
            f = waterbodies.get(fid)
            if objectid == int(f["properties"]["OBJECTID"]):
                metadata = f["properties"]
            else:
                return get_waterbody_properties(objectid=objectid)
        else:
            for f in waterbodies:
                if objectid == int(f["properties"]["OBJECTID"]):
                    metadata = f["properties"]
                    break
    return metadata


def get_waterbody_objectids():
    objectids = []
    with fiona.open(WATERBODY_DBF) as waterbodies:
        for f in waterbodies:
            objectids.append(int(f["properties"]["OBJECTID"]))
    return objectids


def get_waterbody_count():
    n = 0
    with fiona.open(WATERBODY_DBF) as waterbodies:
        n = len(waterbodies)
    return n


def get_county_boundary(geoid, retry: bool = False):
    geoid_alt = geoid.zfill(5)
    with fiona.open(COUNTY_DBF) as counties:
        crs = counties.crs
        for c in counties:
            if geoid == c["properties"]["GEOID"] or geoid_alt == c["properties"]["GEOID"]:
                return c, crs
    return None, None


def get_state_boundary(state):
    with fiona.open(STATE_DBF) as states:
        crs = states.crs
        for c in states:
            if state == c["properties"]["STUSPS"]:
                return c, crs
    return None, None


def get_tribe_boundary(tribe):
    tribe_alt = tribe.zfill(4)
    with fiona.open(TRIBE_DBF) as tribes:
        crs = tribes.crs
        for c in tribes:
            if tribe == c["properties"]["GEOID"] or tribe_alt == c["properties"]["GEOID"]:
                return c, crs
    return None, None
