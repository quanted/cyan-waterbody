import numpy as np
from pathlib import PurePath
from flaskr.raster import get_images, clip_raster, mosaic_rasters, get_colormap, get_dataset_reader
from flaskr.geometry import get_waterbody
from flaskr.db import get_tiles_by_objectid, get_conn, save_data
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import multiprocessing as mp
import logging
import time
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-waterbody")

N_LIMIT = 2000      # Set the chunk size for slipping up the features for aggregation, reduces memory requirements


def aggregate(year: int, day: int, daily: bool = True, objectid: str = None, offset: int = None):
    """
    Aggregate the images provided in IMAGE_DIR for a specified comid, using the waterbody bounds to tile mapping.
    :param year: The year of the images to process.
    :param day: The day of the year of the images to process.
    :param daily: Defaults to True, otherwise aggregate weekly data.
    :param objectid: The objectid of the waterbody being aggregated.
    :return: The histrogram of pixel values and their count.
    """
    images = get_images(year=year, day=day, daily=daily)
    if len(images) == 0:
        return None
    features, crs = get_waterbody(objectid=objectid)
    n_features = len(features)
    completed = False
    if offset is None:
        new_offset = N_LIMIT if len(features) >= N_LIMIT else len(features)
        features = features[:new_offset]
        print("Aggregating features from index {} -> {}".format(0, new_offset))
        offset = new_offset
    else:
        new_offset = offset + N_LIMIT if len(features) >= offset + N_LIMIT else len(features)
        features = features[offset: new_offset]
        print("Aggregating features from index {} -> {}".format(offset, new_offset))
        offset = new_offset
    if new_offset == n_features:
        completed = True

    f_results = {}
    image_base = PurePath(images[0]).parts[-1].split(".tif")
    image_base = "_".join(image_base[0].split("_")[:-2])
    for i in tqdm(range(len(features)), desc="Aggregating waterbodies..."):
        f = features[i]
        objectid = f["properties"]["OBJECTID"]
        f_results[objectid] = []
        if f["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in f["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
        else:
            poly = gpd.GeoSeries(Polygon(f["geometry"]["coordinates"][0]), crs=crs)
        f_images = get_tiles_by_objectid(objectid, image_base)
        if len(f_images) == 0:
            f_results[objectid] = [np.zeros(255), "FAILED", "No images found for provided OBJECTID"]
            continue
        results = np.zeros(255)
        for i in f_images:
            data = clip_raster(i, poly, boundary_crs=crs)
            if data:
                results = np.add(results, np.histogram(data[0], bins=255)[0])
        f_results[objectid] = [results, "PROCESSED", ""]
    return f_results, offset, completed


def p_aggregate(year: int, day: int, daily: bool = True, objectid: str = None, offset: int = None):
    """
    Aggregate the images provided in IMAGE_DIR for a specified comid, using the waterbody bounds to tile mapping.
    :param year: The year of the images to process.
    :param day: The day of the year of the images to process.
    :param daily: Defaults to True, otherwise aggregate weekly data.
    :param objectid: The objectid of the waterbody being aggregated.
    :return: The histrogram of pixel values and their count.
    """
    images = get_images(year=year, day=day, daily=daily)
    if len(images) == 0:
        return None, None, None
    features, crs = get_waterbody(objectid=objectid)
    n_features = len(features)
    completed = False
    if offset is None:
        new_offset = N_LIMIT if len(features) >= N_LIMIT else len(features)
        features = features[:new_offset]
        print("Aggregating features from index {} -> {}".format(0, new_offset))
        offset = new_offset
    else:
        new_offset = offset + N_LIMIT if len(features) >= offset + N_LIMIT else len(features)
        features = features[offset: new_offset]
        print("Aggregating features from index {} -> {}".format(offset, new_offset))
        offset = new_offset
    if new_offset == n_features:
        completed = True
    image_base = PurePath(images[0]).parts[-1].split(".tif")
    image_base = "_".join(image_base[0].split("_")[:-2])

    cpus = mp.cpu_count() - 2 if mp.cpu_count() - 2 >= 2 else mp.cpu_count()
    pool = mp.Pool(cpus)
    logger.info("Running async, cores: {}".format(cpus))

    results_objects = [pool.apply_async(p_feature_aggregate, args=(f, image_base, crs)) for f in features]
    results = {}
    for i in tqdm(range(len(results_objects)), desc="Aggregating {} data by waterbodies...".format("daily" if daily else "weekly"), ascii=False):
        r = results_objects[i].get()
        results[r[0]] = [r[1], r[2], r[3]]
    return results, offset, completed


def p_feature_aggregate(feature, image_base, crs):
    objectid = feature["properties"]["OBJECTID"]
    results = np.zeros(255)
    f_images = get_tiles_by_objectid(objectid, image_base)
    if len(f_images) == 0:
        return objectid, results, "FAILED", "No images found for the objectID"
    if feature["geometry"]["type"] == "MultiPolygon":
        poly_geos = []
        for p in feature["geometry"]["coordinates"]:
            poly_geos.append(Polygon(p[0]))
        poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
    else:
        poly = gpd.GeoSeries(Polygon(feature["geometry"]["coordinates"][0]), crs=crs)
    if poly.geometry is None:
        return objectid, results, "FAILED", "Geometry unable to be loaded for objectid."
    for i in f_images:
        data = clip_raster(i, poly, boundary_crs=crs)
        if data:
            results = np.add(results, np.histogram(data[0], bins=255)[0])
    return objectid, results, "PROCESSED", ""


def retry_failed(daily: bool = True):
    conn = get_conn()
    cur = conn.cursor()
    if daily:
        query = "SELECT year, day, OBJECTID FROM DailyStatus WHERE status='FAILED'"
    else:
        query = "SELECT year, day, OBJECTID FROM WeeklyStatus WHERE status='FAILED'"
    cur.execute(query)
    failed_total = []
    for r in cur.fetchall():
        failed = {
            "year": r[0],
            "day": r[1],
            "objectid": r[2]
        }
        failed_total.append(failed)
    print("Current {} fail count: {}".format("daily" if daily else "weekly", len(failed_total)))
    for i in tqdm(range(len(failed_total)), desc="Retrying failed aggregations", ascii=False):
        f = failed_total[i]
        year = int(f['year'])
        day = int(f['day'])
        data = aggregate(year=year, day=day, daily=daily, objectid=f['objectid'])[0]
        save_data(year=year, day=day, data=data, daily=daily)


def get_waterbody_raster(objectid: int, year: int, day: int):
    features, crs = get_waterbody(objectid=objectid)
    if len(features) == 0:
        return None, None
    images = get_images(year=year, day=day, daily=True)
    if len(images) == 0:
        return None, None
    image_base = PurePath(images[0]).parts[-1].split(".tif")
    image_base = "_".join(image_base[0].split("_")[:-2])
    f = features[0]
    objectid = f["properties"]["OBJECTID"]
    if f["geometry"]["type"] == "MultiPolygon":
        poly_geos = []
        for p in f["geometry"]["coordinates"]:
            poly_geos.append(Polygon(p[0]))
        poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
    else:
        poly = gpd.GeoSeries(Polygon(f["geometry"]["coordinates"][0]), crs=crs)
    f_images = get_tiles_by_objectid(objectid, image_base)
    if len(f_images) > 1:
        mosaic = mosaic_rasters(f_images)
    else:
        mosaic = f_images[0]
    colormap = get_colormap(f_images[0])
    data = clip_raster(mosaic, poly, boundary_crs=crs, raster_crs={'init': 'epsg:3857'})
    return data, colormap

