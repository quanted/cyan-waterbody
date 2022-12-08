import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import fiona
import os
import geopandas as gpd
import random
import requests
import json
import numpy as np
from shapely.geometry import Polygon, MultiPolygon, Point
from pyproj import Proj, transform

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


def get_waterbody_by_fids(fid: int = None, fids: list = None, tojson: bool = False, name_only: bool = False):
    features = []
    names = {}
    
    logging.warning("get_waterbody_by_fids() called")

    if fid is None and fids is None:
        logging.warning("fid is None and fids is None, returning features: {}".format(features))
        return features
    if tojson:
        logging.warning("Opening WATERBODY_DBF")
        with fiona.open(WATERBODY_DBF) as waterbodies:
            logging.warning("waterbodies: {}".format(waterbodies))
            crs = waterbodies.crs
            logging.warning("crs: {}".format(crs))
            if fid is not None:
                logging.warning("getting fid")
                f = waterbodies.get(fid)
                logging.warning("f: {}".format(f))
                features.append(f)
            if fids is not None:
                logging.warning("getting fids")
                for _fid in fids:
                    logging.warning("fid: {}".format(_fid))
                    f = waterbodies.get(_fid)
                    logging.warning("f: {}".format(f))
                    features.append(f)
            logging.warning("features: {}".format(features))
            geojson = []
            for feature in features:
                logging.warning("Looping feature: {}".format(feature))
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
            if fid is not None:
                f = waterbodies.get(fid)
                features.append(f)
                names[int(f["properties"]["OBJECTID"])] = f["properties"]["GNIS_NAME"]
            if fids is not None:
                for _fid in fids:
                    f = waterbodies.get(_fid)
                    features.append(f)
                    names[int(f["properties"]["OBJECTID"])] = f["properties"]["GNIS_NAME"]
        if name_only:
            return names
        return features, crs


def get_waterbody(objectid: int = None, objectids: list = None, tojson: bool = False, all: bool = False):
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
        features_list = []
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
                elif all:
                    features_list.append(f)
                else:
                    features.append(f)
        if all:
            feature_dict = {
                "type": "FeatureCollection",
                "features": features_list
            }
            features = gpd.GeoDataFrame.from_features(feature_dict, crs=crs)
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
    logging.warning("inside get_waterbody_byID, id: {}, fid: {}".format(id, fid))
    if id is None and fid is None:
        return {}
    waterbody = []
    t0 = time.time()
    if not fid:
        logging.warning("No fid Opening WATERBODY_DBF")
        with fiona.open(WATERBODY_DBF) as waterbodies:
            for f in waterbodies:
                logging.warning("Waterbody: {}".format(f))
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
        logging.warning("with fid Opening WATERBODY_DBF")
        with fiona.open(WATERBODY_DBF) as waterbodies:
            f = waterbodies.get(fid)
            logging.warning("waterbodies: {}".format(f))
            logging.warning("Id: {}".format(id))
            logging.warning('f["properties"]["OBJECTID"]: {}'.format(f["properties"]["OBJECTID"]))
            if int(f["properties"]["OBJECTID"]) == id:
                wb = {
                    "name": f["properties"]["GNIS_NAME"],
                    "objectid": int(f["properties"]["OBJECTID"]),
                    "centroid_lat": float(f["properties"]["c_lat"]),
                    "centroid_lng": float(f["properties"]["c_lng"]),
                    "areasqkm": float(f["properties"]["AREASQKM"]),
                    "state_abbr": f["properties"]["STATE_ABBR"]
                }
                logging.warning("Found wb: {}".format(wb))
                waterbody = [wb]
            else:
                logging.warning("Trying to run get_waterbody_byID again.")
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


def get_waterbody_elevation(fid: int, n: int = 10, delay: int = 2, countdown: int = 0):
    waterbody, crs = get_waterbody_by_fids(fid=fid)
    waterbody = waterbody[0]
    if waterbody["geometry"]["type"] == "MultiPolygon":
        poly_geos = []
        for p in waterbody["geometry"]["coordinates"]:
            poly_geos.append(Polygon(p[0]))
        poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
    else:
        poly = gpd.GeoSeries(Polygon(waterbody["geometry"]["coordinates"][0]), crs=crs)
    points = []
    m = 0
    poly_bounds = poly.geometry.bounds
    while m < n:
        point = Point(random.uniform(poly_bounds['minx'][0], poly_bounds['maxx'][0]), random.uniform(poly_bounds['miny'][0], poly_bounds['maxy'][0]))
        if poly.contains(point)[0]:
            points.append(point)
            m += 1
    del poly
    missing_data = -9999
    bad_request = -9998
    elevations = []
    for point in points:
        query_url = f"https://nationalmap.gov/epqs/pqs.php?x={point.x}&y={point.y}&units=Feet&output=json"
        try:
            response = json.loads(requests.get(query_url).content)
        except Exception:
            if countdown == 0:
                return bad_request, fid
            time.sleep(delay)
            return get_waterbody_elevation(fid=fid, delay=delay+2, countdown=countdown-1)
        elev = response["USGS_Elevation_Point_Query_Service"]["Elevation_Query"]["Elevation"]
        if elev != '-1000000':
            elevations.append(elev)
    if len(elevations) == 0:
        return missing_data, fid
    del points
    min_elevation = round(np.min(elevations), 2)
    return fid, min_elevation


def convert_coordinates(x, y, in_crs, out_crs='4326'):
    in_proj = Proj(init=in_crs)
    out_proj = Proj(init=f'epsg:{out_crs}')
    new_x, new_y = transform(in_proj, out_proj, x, y)
    return new_x, new_y
