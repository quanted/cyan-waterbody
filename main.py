import os
import re
import json
import time
import numpy as np
import rasterio
from rasterio import mask
from rasterio.merge import merge
from rasterio.plot import show, show_hist
from pathlib import PurePath
import fiona
import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import sqlite3

IMAGES_DIR = "D:\data\cyan_rare\RARE\L20203412020347.L3m_7D_CYAN_CI_cyano_CYAN_CONUS_300m"
WATERBODY_DBF = "D:\data\cyan_rare\RARE\lakes-conus\LakeswithStats11_14.dbf"
DB_FILE = ".\\waterbody-data.db"
N_VALUES = 255

def get_images(image_dir: str, mosaic: bool = False):
    if mosaic:
        image_files = [str(os.path.join(image_dir, f)) for f in os.listdir(image_dir) if (".tif" in f)]
        output_file = os.path.join(image_dir, "{}.mosaic.tif".format(re.split(r"[\\,/]\s*", image_dir)[-1]))
        images = []
        out_meta = None
        crs = None
        for i in image_files:
            src = rasterio.open(i)
            out_meta = src.meta.copy()
            images.append(src)
            crs = src.crs
        mosaic, out_trans = merge(images)
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "crs": json.dumps(crs.data)
        })
        # show(mosaic)
        with rasterio.open(output_file, "w", **out_meta) as output:
            output.write(mosaic)
        image_files = [output_file]
    else:
        image_files = [str(os.path.join(image_dir, f)) for f in os.listdir(image_dir) if (".tif" in f and "mosaic" not in f)]
    return image_files


def get_waterbody(comid: str = None):
    features = []
    with fiona.open(WATERBODY_DBF) as waterbodies:
        crs = waterbodies.crs
        for f in waterbodies:
            if comid:
                if comid == f["properties"]["COMID"]:
                    features.append(f)
                    continue
            else:
                features.append(f)
    return features, crs


def clip_raster(raster, boundary, boundary_layer=None, boundary_crs=None):
    """Clip the raster to the given boundary.

    Parameters
    ----------
    raster : string, pathlib.Path or rasterio.io.DataSetReader
        Location of or already opened raster.
    boundary : string, pathlib.Path or geopandas.GeoDataFrame
        The polygon by which to clip the raster.
    boundary_layer : string, optional
        For multi-layer files (like GeoPackage), specify the layer to be used.


    Returns
    -------
    tuple
        Three elements:
            clipped : numpy.ndarray
                Contents of clipped raster.
            affine : affine.Affine()
                Information for mapping pixel coordinates
                to a coordinate system.
            crs : dict
                Dict of the form {'init': 'epsg:4326'} defining the coordinate
                reference system of the raster.

    """

    if isinstance(raster, Path):
        raster = str(raster)
    if isinstance(raster, str):
        raster = rasterio.open(raster)

    if isinstance(boundary, Path):
        boundary = str(boundary)
    if isinstance(boundary, str):
        if ".gpkg" in boundary:
            driver = "GPKG"
        else:
            driver = None  # default to shapefile
            boundary_layer = ""  # because shapefiles have no layers

        boundary = gpd.read_file(boundary, layer=boundary_layer, driver=driver)

    if not (boundary_crs == raster.crs or boundary_crs == raster.crs.data):
        boundary = boundary.to_crs(crs=raster.crs)
    coords = [boundary["geometry"]]

    # mask/clip the raster using rasterio.mask
    try:
        clipped, affine = mask.mask(dataset=raster, shapes=coords, crop=True)
    except Exception as e:
        print("ERROR: {}".format(e))
        return None

    if len(clipped.shape) >= 3:
        clipped = clipped[0]

    return clipped, affine, raster.crs


def get_tiles_by_comid(comid: str, image_base: str, image_dir: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT tileName FROM GeometryTile WHERE comid=?"
    values = (comid,)
    cur.execute(query, values)
    tiles = cur.fetchall()
    cur.close()
    images = []
    for i in tiles:
        images.append(os.path.join(image_dir, image_base + "_" + i[0] + ".tif"))
    return images


def aggregate(image_dir: str = None, mosaic: bool = False, save: bool = False, plot: bool = False, comid: str = None, mapped_tiles: bool = False):
    if not image_dir:
        image_dir = IMAGES_DIR
    images = get_images(image_dir, mosaic)
    features, crs = get_waterbody(comid)
    f_results = {}
    image_base = PurePath(images[0]).parts[-1].split(".tif")
    image_base = "_".join(image_base[0].split("_")[:-2])
    for f in features:
        comid = f["properties"]["COMID"]
        if mapped_tiles:
            images = get_tiles_by_comid(comid, image_base, image_dir)
        for i in images:
            data = clip_raster(i, f, boundary_crs=crs)
            if data:
                if plot:
                    show(data[0])
                    show_hist(source=data[0], bins=255, title="Histogram for COMID: {}".format(f["properties"]["COMID"]), histtype='stepfilled', alpha=0.5)
                if comid in f_results.keys():
                    f_results[comid] = f_results[comid] + np.histogram(data[0], bins=255)[0]
                else:
                    f_results[comid] = np.histogram(data[0], bins=255)[0]
        if save:
            f_results[comid].tofile('{}-histogram-data.csv'.format(comid), sep=',')
    return f_results


def save_data(year, day, data):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("BEGIN")
    insert_i = 1
    max_i = 400
    for c, d in data.items():
        for i in range(1, d.size - 1):
            if d[i] > 0:
                query = "INSERT INTO WeeklyData(year, day, comid, value, count) VALUES(?,?,?,?,?)"
                values = (year, day, c, i, int(d[i]),)
                cur.execute(query, values)
            if insert_i % max_i == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
                insert_i = 1
            else:
                insert_i += 1
    cur.execute("COMMIT")
    conn.close()


def compare_histograms(file_path):
    data = pd.read_csv(file_path, sep=',', header=None)
    x = np.arange(data.shape[1])
    y = (data.iloc[[0]].to_numpy() - data.iloc[[1]].to_numpy())[0]
    plt.plot(x, y)
    plt.ylabel("Histogram Difference")
    plt.show()


def set_geometry_tiles():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("BEGIN")
    features, crs = get_waterbody()
    images = get_images(IMAGES_DIR)
    insert_i = 1
    max_i = 400
    for f in features:
        comid = f["properties"]["COMID"]
        for i in images:
            data = clip_raster(i, f, boundary_crs=crs)
            if data is not None:
                tile_parts = i.split("_")
                tile_name = (tile_parts[-2] + "_" + tile_parts[-1]).split(".")[0]
                query = "INSERT INTO GeometryTile(comid, tileName) VALUES(?,?)"
                values = (comid, tile_name,)
                cur.execute(query, values)
            if insert_i % max_i == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
                insert_i = 1
            else:
                insert_i += 1
    cur.execute("COMMIT")
    conn.close()


def get_waterbody_data(comid: str, start_year: int = None, start_day: int = None, end_year: int = None, end_day: int = None, ranges: list = None):
    """
    Regenerate histogram data from database for a provided waterbody comid.
    :param comid: NHDPlus waterbody comid
    :param start_year: optional start year for histogram
    :param start_day: optional start day for histogram
    :param end_year: optional end year for histogram
    :param end_day: optional end day for histogram
    :param ranges: optional histogram ranges, can correspond to user specified thresholds. Must be formated as a 2d array.
        i.e: [[0:10],[11:100],[101:200],[201:255]].
    :return: a dictionary of dates, year and day of year, and an array with 255 values of cell counts, or the cell counts for the ranges.
    """
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT * FROM WeeklyData WHERE comid=?"
    values = [comid]
    if start_year:
        query = query + " AND year >= ?"
        values.append(start_year)
    if start_day:
        query = query + " AND day >= ?"
        values.append(start_day)
    if end_year:
        query = query + " AND year <= ?"
        values.append(end_year)
    if end_day:
        query = query + " AND day <= ?"
        values.append(end_day)
    cur.execute(query, tuple(values))
    data_rows = cur.fetchall()
    data = {}
    for r in data_rows:
        day = str(r[0]) + " " + str(r[1])
        if day not in data.keys():
            histogram = np.zeros(N_VALUES)
            data[day] = histogram
        data[day][r[3]] = r[4]
    cur.close()
    if ranges:
        range_data = {}
        for r in ranges:
            for date in data.keys():
                if date in range_data.keys():
                    range_data[date].append(int(np.sum(data[date][r[0]:r[1]])))
                else:
                    range_data[date] = [int(np.sum(data[date][r[0]:r[1]]))]
        data = range_data
    return data


if __name__ == '__main__':
    # year = 2021
    # day = 47
    comid = "166757656"
    test_range = [[0, 50], [50, 100], [100, 150], [150, 200], [200, 255]]

    images_dir = "D:\data\cyan_rare\RARE\L20201872020193.L3m_7D_CYAN_CI_cyano_CYAN_CONUS_300m"
    year = 2020
    day = 193

    t0 = time.time()

    # runtime 836.4315 sec
    # data = aggregate(mosaic=False)

    # runtime 178.7156 sec, 39286
    # data = aggregate(mosaic=True)

    # runtime 177.2146 sec, 40218
    # data = aggregate(image_dir=images_dir, mapped_tiles=True)
    # save_data(year, day, data)

    # comid_data = get_waterbody_data(comid, ranges=test_range)
    end_year = 2020
    comid_data = get_waterbody_data(comid, ranges=test_range, end_year=end_year)

    # compare_histograms(".\\166757656-histogram-data.csv")
    t1 = time.time()
    print("Runtime: {} sec".format(round(t1-t0, 4)))

#
# Bare DB		0.60Mb		0mb
# Weekly +1	    2.06Mb		2.0Mb
# Weekly +2	    3.67Mb		1.61Mb
# Weekly +3	    5.83Mb		2.16Mb
# Weekly +4	    7.10Mb		1.27Mb
#
# round to ~2Mb a date.
#
# Weekly data would ~ be 104Mb/year
# Daily data would ~ be 730Mb/year

