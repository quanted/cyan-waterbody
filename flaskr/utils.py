import copy
import os
import sqlite3
from tqdm import tqdm
import multiprocessing as mp
from flaskr.aggregate import get_waterbody_raster
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-waterbody")

# Default colormap
colormap = {
    0: (255, 255, 255, 0.5),                    # below detection
    254: (175, 125, 45, 1),                     # land
    255: (160, 187, 91, 0.21)                   # no data
}

# Colormap colors
rbga = {
    'low': (0, 128, 0, 1),
    'medium': (200, 200, 0, 1),
    'high': (255, 165, 0, 1),
    'vhigh': (255, 0, 0, 1)
}

DB_FILE = os.path.join(os.getenv("WATERBODY_DB", "D:\\data\cyan_rare\\mounts\\database"), "waterbody-data.sqlite")


def get_colormap(low: int = 100, med: int = 140, high: int = 183):
    new_colormap = copy.copy(colormap)
    for i in range(1, low, 1):
        new_colormap[i] = rbga['low']
    for i in range(low + 1, med, 1):
        new_colormap[i] = rbga['medium']
    for i in range(med + 1, high, 1):
        new_colormap[i] = rbga['high']
    for i in range(high, 254, 1):
        new_colormap[i] = rbga['vhigh']
    return new_colormap


def update_geometry_bounds(day: int, year: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT OBJECTID FROM GeometryIndex"
    cur.execute(query)
    objectids = cur.fetchall()
    cur.execute("BEGIN")
    for i in tqdm(range(len(objectids)), desc="Settings geometry bounds in db...", ascii=False):
        objectid = objectids[i][0]
        data, cm = get_waterbody_raster(objectid=objectid, day=day, year=year)
        raster, trans, crs, bounds = data
        query = "UPDATE WaterbodyBounds Set x_min=?, x_max=?, y_min=?, y_max=? WHERE OBJECTID=?"
        values = (bounds[1][1], bounds[0][1], bounds[0][0], bounds[1][0], objectid,)
        cur.execute(query, values)
    cur.execute("COMMIT")


def p_update_geometry_bounds(day: int, year: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT OBJECTID FROM GeometryIndex"
    cur.execute(query)
    objectids = cur.fetchall()

    cpus = mp.cpu_count() - 2 if mp.cpu_count() - 2 >= 2 else mp.cpu_count()
    pool = mp.Pool(cpus)
    logger.info("Running async, cores: {}".format(cpus))
    results = {}
    results_objects = [pool.apply_async(p_get_geometry_bounds, args=(objectid[0], day, year)) for objectid in objectids]
    for i in tqdm(range(len(results_objects)), desc="Settings geometry bounds in db...", ascii=False):
        r = results_objects[i].get()
        results[r[4]] = [r[0], r[1], r[2], r[3]]
    cur.execute("BEGIN")
    i = 0
    for objectid, bounds in results.items():
        query = "UPDATE WaterbodyBounds Set x_min=?, x_max=?, y_min=?, y_max=? WHERE OBJECTID=?"
        values = (bounds[0], bounds[1], bounds[2], bounds[3], objectid,)
        cur.execute(query, values)
        if i % 400 == 0:
            cur.execute("COMMIT")
            cur.execute("BEGIN")
        i += 1
    cur.execute("COMMIT")


def p_get_geometry_bounds(objectid, day, year):
    data, cm = get_waterbody_raster(objectid=objectid, day=day, year=year)
    raster, trans, crs, bounds = data
    values = (bounds[1][1], bounds[0][1], bounds[0][0], bounds[1][0], objectid,)
    return values


