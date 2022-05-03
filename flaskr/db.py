import os
import sqlite3
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, shape
from flaskr.geometry import get_waterbody, get_waterbody_count, get_waterbody_by_fids
from flaskr.raster import get_images, clip_raster, get_images_by_tile, get_raster_bounds
import datetime
from tqdm import tqdm
import multiprocessing as mp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-waterbody")

IMAGE_DIR = os.getenv('IMAGE_DIR', "D:\\data\cyan_rare\\mounts\\images")
DB_FILE = os.path.join(os.getenv("WATERBODY_DB", "D:\\data\cyan_rare\\mounts\\database"), "waterbody-data_0.2.sqlite")
N_VALUES = 256

BAD_OBJECTIDS = [8439286, 7951918, 3358607, 3012931, 2651373, 480199]


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    return conn


def get_tiles_by_objectid(objectid: str, image_base: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT tileName FROM GeometryTile WHERE OBJECTID=?"
    values = (objectid,)
    cur.execute(query, values)
    tiles = cur.fetchall()
    conn.close()
    images = []
    for i in tiles:
        images.append(os.path.join(IMAGE_DIR, image_base + "_" + i[0] + ".tif"))
    return images


def get_waterbody_data(objectid: str, daily: bool = True, start_year: int = None, start_day: int = None,
                       end_year: int = None, end_day: int = None, ranges: list = None, non_blooms: bool = False):
    """
    Regenerate histogram data from database for a provided waterbody objectid.
    :param objectid: NHD HR waterbody OBJECTID
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
    if daily:
        query = "SELECT * FROM DailyData WHERE OBJECTID=?"
    else:
        query = "SELECT * FROM WeeklyData WHERE OBJECTID=?"
    values = [objectid]
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
    conn.close()
    if ranges:
        range_data = {}
        ranges.append([ranges[-1][1], 254])
        count_test = {}
        for r in ranges:
            for date in data.keys():
                count_test[date] = np.sum(data[date][1:254])
                if date in range_data.keys():
                    range_data[date].append(int(np.sum(data[date][r[0]:r[1]])))
                else:
                    range_data[date] = [int(np.sum(data[date][r[0]:r[1]]))]
        if non_blooms:
            # Add count values for DN=0, DN=254 and DN=255, after the bloom values, so are in indices 4, 5, and 6
            for date in data.keys():
                range_data[date].append(int(data[date][0]))
                range_data[date].append(int(data[date][254]))
                range_data[date].append(int(data[date][255]))
        data = range_data
    results = {}
    for date, array in data.items():
        results[date] = np.array(array).tolist()
    return results


def get_custon_waterbody_data(geojson, daily: bool = True, start_year: int = None, start_day: int = None, end_year: int = None, end_day: int = None, ranges: list = None):
    """
    Process histogram data for a provided geojson.
    :param geojson: User provided geojson
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
    try:
        poly = shape(geojson)
    except Exception as e:
        logger.fatal("Unable to convert geojson to polygon. Error: {}".format(e))
        return None
    data = {}
    # TODO: 1. Add function for getting the tiles which contain the polygon (may have to create polygon boxes from tileBounds and check each if there is any overlap
    # TODO: 2. Get images for the tiles listed using function get_images_by_tile()
    # TODO: 3. Iterate over list of images and aggregate.
    if ranges:
        range_data = {}
        for r in ranges:
            for date in data.keys():
                if date in range_data.keys():
                    range_data[date].append(int(np.sum(data[date][r[0]:r[1]])))
                else:
                    range_data[date] = [int(np.sum(data[date][r[0]:r[1]]))]
        data = range_data
    results = {}
    for date, array in data.items():
        results[date] = np.array(array).tolist()
    return results


def get_waterbody_fid(objectid: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT FID FROM WaterbodyBounds WHERE OBJECTID==?"
    cur.execute(query, (int(objectid),))
    fid = cur.fetchall()
    return fid[0][0]


def get_waterbody_bypoint(lat: float, lng: float, return_fid: bool=False):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT OBJECTID, FID FROM WaterbodyBounds WHERE y_max>=? AND x_min<=? AND y_min<=? AND x_max>=?"
    values = (lat, lng, lat, lng,)
    cur.execute(query, values)
    lakes = cur.fetchall()
    crs = None
    if len(lakes) > 0:
        features = []
        for lake in lakes:
            # w = get_waterbody(int(lake[0]))
            w = get_waterbody_by_fids(fid=lake[1])
            features.append(w[0][0])
            crs = w[1]
        wb = (features, crs)
    else:
        return None, None
    objectid = None
    gnis_name = None
    point = gpd.GeoSeries(Point(lng, lat), crs='EPSG:4326').to_crs(wb[1])
    for features in wb[0]:
        if features["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in features["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=wb[1])
        else:
            poly = gpd.GeoSeries(Polygon(features["geometry"]["coordinates"][0]), crs=wb[1])
        in_wb = poly.contains(point)
        if in_wb.loc[0]:
            objectid = features["properties"]["OBJECTID"]
            gnis_name = features["properties"]["GNIS_NAME"]
            break
    conn.close()
    if return_fid:
        return objectid, get_waterbody_fid(objectid), gnis_name
    else:
        return objectid, gnis_name


def get_waterbody_bounds(objectid: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT * FROM WaterbodyBounds WHERE OBJECTID=?"
    values = (objectid,)
    cur.execute(query, values)
    bounds = cur.fetchone()
    return bounds


def save_data(year, day, data, daily: bool = True):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("BEGIN")
    insert_i = 1
    max_i = 400
    objectids = list(data.keys())
    for i in tqdm(range(len(objectids)), desc="Saving aggregation data to database...", ascii=False):
        c = objectids[i]
        d = data[c][0]
        status = data[c][1]
        message = data[c][2]
        update_status(cur, year=year, day=day, objectid=c, daily=daily, status=status, comments=message)
        insert_i += 1
        if status == "FAILED":
            continue
        elif status == "PROCESSED":
            for i in range(0, d.size-1):
                if d[i] > 0:
                    if daily:
                        query = "INSERT OR REPLACE INTO DailyData(year, day, OBJECTID, value, count) VALUES(?,?,?,?,?)"
                    else:
                        query = "INSERT OR REPLACE INTO WeeklyData(year, day, OBJECTID, value, count) VALUES(?,?,?,?,?)"
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


def set_geometry_tiles(year: int, day: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    # clear existing GeometryTile table
    cur.execute("DELETE FROM GeometryTile")
    cur.execute("COMMIT")
    cur.execute("BEGIN")
    features, crs = get_waterbody()
    images = get_images(year, day)
    insert_i = 1
    max_i = 400
    for i in tqdm(range(len(features)), desc="Settings geometry to tiff tile mappings...", ascii=False):
        f = features[i]
        objectid = int(f["properties"]["OBJECTID"])
        if f["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in f["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
        else:
            poly = gpd.GeoSeries(Polygon(f["geometry"]["coordinates"][0]), crs=crs)
        for i in images:
            data = clip_raster(i, poly, boundary_crs=crs)
            if data is not None:
                tile_parts = i.split("_")
                tile_name = (tile_parts[-2] + "_" + tile_parts[-1]).split(".")[0]
                query = "INSERT INTO GeometryTile(OBJECTID, tileName) VALUES(?,?)"
                values = (objectid, tile_name,)
                cur.execute(query, values)
            if insert_i % max_i == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
                insert_i = 1
            else:
                insert_i += 1
    cur.execute("COMMIT")
    conn.close()


def p_set_geometry_tiles(year: int, day: int, objectid: int = None):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    # clear existing GeometryTile table
    cur.execute("DELETE FROM GeometryTile")
    cur.execute("COMMIT")
    cur.execute("BEGIN")
    if objectid:
        features, crs = get_waterbody(objectid=objectid)
    else:
        features, crs = get_waterbody()
    images = get_images(year, day)

    cpus = mp.cpu_count() - 2 if mp.cpu_count() - 2 >= 2 else mp.cpu_count()
    pool = mp.Pool(cpus)
    logger.info("Running async, cores: {}".format(cpus))

    results_objects = [pool.apply_async(p_set_tiles, args=(f, crs, images)) for f in features]
    results = []
    for i in tqdm(range(len(results_objects)), desc="Searching for geometry and tif overlapping...", ascii=False):
        results.append(results_objects[i].get())
    insert_i = 1
    max_i = 400
    for i in tqdm(range(len(results)), desc="Uploading tile to geometry mapping to DB...", ascii=False):
        r = results[i]
        for img in r[1]:
            query = "INSERT INTO GeometryTile(OBJECTID, tileName) VALUES(?,?)"
            values = (r[0], img,)
            cur.execute(query, values)
            if insert_i % max_i == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
                insert_i = 1
            else:
                insert_i += 1
    cur.execute("COMMIT")
    conn.close()


def p_set_tiles(feature, crs, images: list):
    objectid = int(feature["properties"]["OBJECTID"])
    if feature["geometry"]["type"] == "MultiPolygon":
        poly_geos = []
        for p in feature["geometry"]["coordinates"]:
            poly_geos.append(Polygon(p[0]))
        poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
    else:
        poly = gpd.GeoSeries(Polygon(feature["geometry"]["coordinates"][0]), crs=crs)
    image_tiles = []
    for i in images:
        data = clip_raster(i, poly, boundary_crs=crs)
        if data is not None:
            tile_parts = i.split("_")
            tile_name = (tile_parts[-2] + "_" + tile_parts[-1]).split(".")[0]
            image_tiles.append(tile_name)
    return objectid, image_tiles


def update_status(cur, year: int, day: int, objectid: str, daily: bool, status: str, comments: str = None):
    timestamp = datetime.datetime.utcnow()
    if daily:
        query = "INSERT OR REPLACE INTO DailyStatus(year, day, OBJECTID, status, timestamp, comments) VALUES(?,?,?,?,?,?)"
    else:
        query = "INSERT OR REPLACE INTO WeeklyStatus(year, day, OBJECTID, status, timestamp, comments) VALUES(?,?,?,?,?,?)"
    values = (year, day, objectid, status, timestamp, comments)
    cur.execute(query, values)


def set_tile_bounds(year: int, day: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("DELETE FROM TileBounds")
    images = get_images(year, day)
    for i in images:
        tile_parts = i.split("_")
        tile_name = (tile_parts[-2] + "_" + tile_parts[-1]).split(".")[0]
        bounds = get_raster_bounds(i)
        query = "INSERT INTO TileBounds(tile, x_min, x_max, y_min, y_max) VALUES(?,?,?,?,?)"
        values = (tile_name, bounds[0], bounds[2], bounds[1], bounds[3])
        cur.execute(query, values)
    cur.execute("COMMIT")
    conn.close()


def set_index(objectid_i: list):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("DELETE FROM GeometryIndex")
    for entity in objectid_i:
        objectid = int(entity[0])
        gnis = entity[1]
        index = entity[2]
        query = "INSERT INTO GeometryIndex VALUES (?,?,?)"
        values = (objectid, gnis, index,)
        cur.execute(query, values)
    cur.execute("COMMIT")
    conn.close()


def get_object_index(objectid: int = None, gnis: str = None):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if objectid:
        query = "SELECT idx FROM GeometryIndex WHERE OBJECTID=?"
        values = (objectid,)
    else:
        query = "SELECT idx FROM GeometryIndex WHERE GNIS_NAME=?"
        values = (gnis,)
    cur.execute(query, values)
    index = cur.fetchall()[0]
    conn.close()
    return index


def check_status(day: int, year: int, daily: bool = True):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    # n_query = "SELECT COUNT(ObjectID) FROM WaterbodyBounds"
    # cur.execute(n_query)
    n = get_waterbody_count()
    if daily:
        query = "SELECT * FROM DailyStatus WHERE year=? AND day=?"
    else:
        query = "SELECT * FROM WeeklyStatus WHERE year=? AND day=?"
    values = (year, day,)
    cur.execute(query, values)
    processed = 0
    total = 0
    fails = []
    for r in cur:
        status = r[3]
        if status == "FAILED":
            fails.append(r[2])
        else:
            processed += 1
        total += 1
    completed = round(100 * processed/total, 2) if total > 0 else 0
    status = "COMPLETED" if processed == total and total == n else "INCOMPLETED" if 100 > completed > 0 else "UNKNOWN"
    status = "FAILED" if len(fails) == total and total > 0 else status
    results = {
        "day": day,
        "year": year,
        "daily": daily,
        "total": total,
        "completed": f"{completed}%",
        "failed": fails,
        "status": status
    }
    conn.close()
    return results


def check_overall_status(start_day: int, start_year: int, end_day: int, end_year: int, daily: bool = True):
    results = {}
    for year in range(start_year, end_year + 1):
        day0 = 1
        day1 = 365
        leap = 1 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 0
        if year == start_year:
            day0 = start_day
        if year == end_year:
            day1 = end_day
        for day in range(day0, day1 + 1 + leap):
            day_results = check_status(day=day, year=year, daily=daily)
            results[f"{year} {day}"] = day_results
    return results


def check_images(year: int, day: int, daily: bool=True):
    images = get_images(year=year, day=day, daily=daily)
    if len(images) > 0:
        return True
    else:
        return False


def get_conus_objectids():
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT STUSPS FROM WaterBodyState"
    cur.execute(query)
    states = sorted(cur.fetchall())
    for state in states:
        query = f"SELECT DISTINCT OBJECTID FROM WaterBodyState WHERE STUSPS=?"
        value = (state[0],)
        cur.execute(query, value)
        results[state] = []
        for w in cur.fetchall():
            if w not in BAD_OBJECTIDS:
                results[state].append(w)
    conn.close()
    return results


def get_eparegion_objectids(regions: list):
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    regions = sorted(regions)
    for region in regions:
        query = "SELECT DISTINCT OBJECTID FROM WaterBodyState WHERE EPAREGION=?"
        value = (region,)
        cur.execute(query, value)
        results[region] = []
        for w in cur.fetchall():
            if w[0] not in BAD_OBJECTIDS:
                results[region].append(w[0])
    conn.close()
    return results


def get_state_objectids(states: list, with_counties: bool = True):
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    states = sorted(states)
    for state in states:
        if with_counties:
            query = "SELECT DISTINCT GEOID FROM WaterBodyCounty WHERE STUSPS=? ORDER BY NAME ASC"
            value = (state,)
            cur.execute(query, value)
            results[state] = {}
            counties = [county[0] for county in list(cur.fetchall())]
            results[state] = get_county_objectids(counties=counties)
        else:
            query = "SELECT DISTINCT OBJECTID FROM WaterBodyState WHERE STUSPS=?"
            values = (state,)
            cur.execute(query, values)
            results[state] = []
            for c in cur.fetchall():
                results[state].append(c[0])
    conn.close()
    return results


def get_county_geoid(county_name, state):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT GEOID FROM WaterBodyCounty WHERE NAMELSAD=? AND STUSPS=?"
    value = (county_name, state,)
    cur.execute(query, value)
    result = cur.fetchall()
    return result[0]


def get_tribe_objectids(tribes: list):
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    tribes = sorted(tribes)
    for tribe in tribes:
        query = "SELECT DISTINCT NAME FROM WaterBodyTribe WHERE GEOID=?"
        value = (tribe,)
        cur.execute(query, value)
        tribe_name = cur.fetchone()[0]
        query = "SELECT DISTINCT OBJECTID FROM WaterBodyTribe WHERE GEOID=?"
        value = (tribe,)
        cur.execute(query, value)
        results[tribe_name] = []
        for w in cur.fetchall():
            if w[0] not in BAD_OBJECTIDS:
                results[tribe_name].append(w[0])
    conn.close()
    return results


def get_county_objectids(counties: list):
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    for county in counties:
        query = "SELECT DISTINCT NAMELSAD FROM WaterBodyCounty WHERE GEOID=?"
        value = (county,)
        cur.execute(query, value)
        county_name = cur.fetchone()[0]
        query = "SELECT DISTINCT OBJECTID FROM WaterBodyCounty WHERE GEOID=?"
        value = (county,)
        cur.execute(query, value)
        results[county_name] = []
        for w in cur.fetchall():
            if w[0] not in BAD_OBJECTIDS:
                results[county_name].append(w[0])
    conn.close()
    return results


def get_group_metrics(objectids: list, year: int, day: int, ranges: dict, p_days: int = 7):
    results = {}
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if objectids is None:
        return results
    objectid_tuple = tuple(objectids) if len(objectids) > 1 else f"({objectids[0]})"

    for r, indx in ranges.items():
        current_date0 = datetime.datetime(year=year, month=1, day=1) + datetime.timedelta(days=day - 1)
        results[r] = {}
        for i in range(0, p_days):
            current_date = current_date0 - datetime.timedelta(days=i)
            i_year = current_date.year
            i_day = current_date.timetuple().tm_yday
            date_key = f"{i_year} {i_day}"
            # the number of waterbodies with any detected cyano  'OBJECTID': {value: count}
            query = f"SELECT DISTINCT OBJECTID FROM DailyData WHERE count>0 AND value>=? AND value<? AND year=? AND day=? AND OBJECTID IN {objectid_tuple}"
            values = (indx[0], indx[1], i_year, i_day,)
            cur.execute(query, values)
            results[r][date_key] = []
            for r0 in cur.fetchall():
                if r0[0] not in BAD_OBJECTIDS:
                    results[r][date_key].append(r0[0])
    conn.close()
    return results


def get_county_state(county_id: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT S.Name FROM WaterBodyState AS S INNER JOIN WaterBodyCounty AS C WHERE S.STUSPS=C.STUSPS AND C.GEOID=?"
    value = (county_id,)
    cur.execute(query, value)
    results = cur.fetchall()
    conn.close()
    return results[0][0]


def get_all_states():
    results = []
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT Name, STUSPS FROM WaterBodyState"
    cur.execute(query)
    for c in cur.fetchall():
        results.append(list(c))
    conn.close()
    return results


def get_all_state_counties(state: str):
    results = []
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT GEOID, NAMELSAD FROM WaterBodyCounty WHERE STUSPS=?"
    value = (state,)
    cur.execute(query, value)
    for c in cur.fetchall():
        c[0] = c[0].zfill(5)
        results.append(list(c))
    conn.close()
    return results


def get_all_tribes():
    results = []
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT GEOID, Name FROM WaterBodyTribe"
    cur.execute(query)
    for c in cur.fetchall():
        c[0] = c[0].zfill(4)
        results.append(list(c))
    conn.close()
    return results


def get_state_name(state):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT NAME FROM WaterBodyState WHERE STUSPS=?"
    value = (str(state),)
    cur.execute(query, value)
    state_name = cur.fetchall()[0][0]
    conn.close()
    return state_name


def get_tribe_geoid(tribe):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = "SELECT DISTINCT GEOID FROM WaterBodyTribe WHERE NAME=?"
    value = (str(tribe),)
    cur.execute(query, value)
    tribe_id = cur.fetchall()[0][0]
    conn.close()
    return tribe_id


def get_states_from_wb(objectids: tuple):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    query = f"SELECT DISTINCT STUSPS FROM WaterBodyState WHERE OBJECTID IN {objectids}"
    query = query.replace(",", "") if len(objectids) == 1 else query
    cur.execute(query)
    states = []
    for r in cur.fetchall():
        states.append(r[0])
    conn.close()
    return states
