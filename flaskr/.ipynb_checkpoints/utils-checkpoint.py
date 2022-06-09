import copy
import os
import sqlite3
from tqdm import tqdm
import numpy as np
import multiprocessing as mp
from flaskr.aggregate import get_waterbody_raster
from flaskr.geometry import get_waterbody_fids
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-waterbody")

# Default colormap
colormap = {
    0: (255, 255, 255, 0.5),                    # below detection
    254: (175, 125, 45, 1),                     # land
    255: (160, 187, 91, 0.21)                   # no data
}
colormap_rgb = {
    0: '(255, 255, 255)',                    # below detection
    254: '(175, 125, 45)',                     # land
    255: '(160, 187, 91)'                   # no data
}

# Colormap colors
rgba = {
    'low': (0, 128, 0, 255),
    'medium': (200, 200, 0, 255),
    'high': (255, 165, 0, 255),
    'vhigh': (255, 0, 0, 255)
}
rgb = {
    'low': (0, 128, 0),
    'medium': (200, 200, 0),
    'high': (255, 165, 0),
    'vhigh': (255, 0, 0)
}

DB_FILE = os.path.join(os.getenv("WATERBODY_DB", "D:\\data\cyan_rare\\mounts\\database"), "waterbody-data_0.2.sqlite")

DEFAULT_RANGE = [
    [1, 100],
    [100, 140],
    [140, 183]
]


def get_colormap(low: int = 100, med: int = 140, high: int = 183, transparency: bool = True):
    if transparency:
        new_colormap = copy.copy(colormap)
        for i in range(1, low, 1):
            new_colormap[i] = rgba['low']
        for i in range(low + 1, med, 1):
            new_colormap[i] = rgba['medium']
        for i in range(med + 1, high, 1):
            new_colormap[i] = rgba['high']
        for i in range(high, 254, 1):
            new_colormap[i] = rgba['vhigh']
    else:
        new_colormap = copy.copy(colormap_rgb)
        for i in range(1, low, 1):
            new_colormap[i] = f"{rgb['low']}"
        for i in range(low + 1, med, 1):
            new_colormap[i] = f"{rgb['medium']}"
        for i in range(med + 1, high, 1):
            new_colormap[i] = f"{rgb['high']}"
        for i in range(high, 254, 1):
            new_colormap[i] = f"{rgb['vhigh']}"
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


def convert_dn(dn, round=2):
    if type(dn) == int or type(dn) == np.int64:
        np.int(64)
        if dn == 0:
            return 0
        if dn > 255:
            print(f"DN value greater than acceptable max, dn: {dn}")
    elif type(dn) == list:
        dn = np.array(dn, dtype=np.int64)
        if np.max(dn) > 255:
            print(f"DN value greater than acceptable max, dn: {np.max(dn)}")
    cell_con = np.around(np.power(10, (3 / 250) * dn - 4.2) * 10 ** 8, round)
    return cell_con


def convert_cc(cell_concentration):
    if cell_concentration < 6000:
        return cell_concentration
    elif cell_concentration == 0:
        return 0
    else:
        dn = round((np.log10(cell_concentration/10**8)+4.2) * (250/3))
        return dn

def update_waterbody_fids():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        # attempt to create new column
        print("Adding FID column to WaterbodyBounds.")
        add_column_query = "ALTER TABLE WaterbodyBounds Add COLUMN FID integer"
        cur.execute(add_column_query)
    except Exception:
        print("WaterbodyBounds table already has a FID column, updating existing values.")

    objects_fids = get_waterbody_fids()

    for i in tqdm(range(len(objects_fids)), desc="Settings geometry waterbody fid in db...", ascii=False):
        query = "UPDATE WaterbodyBounds Set FID=? WHERE OBJECTID=?"
        oid, fid = objects_fids[i]
        values = (fid, oid)
        cur.execute(query, values)
        if i % 400 == 0:
            cur.execute("COMMIT")
            cur.execute("BEGIN")
        i += 1
    cur.execute("COMMIT")


