import numpy as np
from pathlib import PurePath
from flaskr.raster import get_images, clip_raster, mosaic_rasters, get_colormap, get_dataset_reader, rasterize_boundary
from flaskr.geometry import get_waterbody, get_waterbody_by_fids
from flaskr.db import get_tiles_by_objectid, get_conn, save_data, get_waterbody_fid
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import multiprocessing as mp
import logging
import time
import pandas as pd
import copy
import matplotlib.pyplot as plt
from tqdm import tqdm
from PIL import Image
from PIL.PngImagePlugin import PngInfo
import os

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
    df_data = []
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
            f_results[objectid] = [np.zeros(257), "FAILED", "No images found for provided OBJECTID"]
            continue
        results = np.zeros(257)
        for i in f_images:
            data = clip_raster(i, poly, boundary_crs=crs)
            if data:
                results = np.add(results, np.histogram(data[0], bins=257)[0])
        f_results[objectid] = [results, "PROCESSED", ""]
        # df_data.append(list([objectid, f['properties']['AREASQKM'], np.sum(poly.area) * 10**4, round(np.sum(results) * 0.03, 4)]))
    # columns = ["objectid", "wb_area", "wb_geo_area", "wb_pixel_area"]
    # df = pd.DataFrame(df_data, columns=columns)
    # df.plot(x='wb_area', y=['wb_geo_area', 'wb_pixel_area'])
    # plt.show()
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

    # cpus = mp.cpu_count() - 2 if mp.cpu_count() - 2 >= 2 else mp.cpu_count()
    cpus = 2
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
    results = np.zeros(257)
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
            results = np.add(results, np.histogram(data[0], bins=257)[0])
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


def get_waterbody_raster(objectid: int, year: int, day: int, get_bounds: bool = True, retry: int = 5, reproject: bool = True, daily: bool = True):
    fid = get_waterbody_fid(objectid=objectid)
    features, crs = get_waterbody_by_fids(fid=fid)
    if len(features) == 0:
        return None, None
    images = get_images(year=year, day=day, daily=daily)
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
    try:
        data = list(clip_raster(mosaic, poly, boundary_crs=crs, raster_crs={'init': 'epsg:3857'}, histogram=False, get_bounds=get_bounds, reproject=reproject))
    except Exception as e:
        print(f"Error attempting to clip raster for objectid: {objectid}, year: {year}, day: {day}, retry: {retry}, error: {e}")
        if retry > 0:
            data, colormap = get_waterbody_raster(objectid=objectid, year=year, day=day, retry=retry-1, get_bounds=False, reproject=False)
        else:
            return None, colormap
    return data, colormap


def generate_conus_image(year: int, day: int, daily: bool):
    t0 = time.time()
    images = get_images(year=year, day=day, daily=daily)
    mosaic = mosaic_rasters(images, dst_crs={"init": "EPSG:3857"})

    colormap = get_colormap(images[0])
    colormap[0] = (0, 0, 0, 0)
    colormap[254] = (0, 0, 0, 0)
    colormap[255] = (0, 0, 0, 0)

    bounds = None

    data = None
    for r in mosaic:
        bounds = r.bounds
        data = r.read()[0]
    mosaic.close()

    str_bounds = {"bottom": bounds.bottom, "left": bounds.left, "right": bounds.right, "top": bounds.top}

    converted_data = [[(0, 0, 0, 0) for i in range(data.shape[1])] for j in range(data.shape[0])]
    for y in range(0, data.shape[1]):
        for x in range(0, data.shape[0]):
            converted_data[x][y] = list(colormap[data[x][y]])

    converted_data = np.array(converted_data, dtype=np.uint8)

    png_metadata = PngInfo()
    png_metadata.add_text("Bounds", str(str_bounds))
    png_metadata.add_text("Daily", str(daily))
    png_metadata.add_text("Year", str(year))
    png_metadata.add_text("Day", str(day))

    png_img = Image.fromarray(converted_data, mode='RGBA')
    png_img.save(f"{'daily' if daily else 'weekly'}_conus-{year}-{day}.png", 'PNG', pnginfo=png_metadata)
    t1 = time.time()
    logger.info(f"Waterbody CONUS Image Complete, year: {year}, day: {day}, request runtime: {round(t1 - t0, 3)} sec")


def get_conus_file(year: int, day: int, daily: bool, tries: int = 14):
    if tries <= 0:
        return None
    base_path = os.path.join("static", "raster_plots")
    conus_file_name = f"{'daily' if daily else 'weekly'}-conus-{year}-{day}.png"
    conus_file_path = os.path.join(base_path, conus_file_name)
    if os.path.exists(conus_file_path):
        return conus_file_path
    else:
        new_year = year
        if day == 1:
            new_year = new_year - 1
            new_day = 365
        else:
            new_day = day - 1
        return get_conus_file(new_year, new_day, daily, tries-1)