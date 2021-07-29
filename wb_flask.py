import warnings

import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)

from flask import Flask, request, send_file, make_response
from flaskr.db import get_waterbody_data, get_waterbody_bypoint, get_waterbody
from flaskr.geometry import get_waterbody_byname, get_waterbody_properties
from flaskr.aggregate import get_waterbody_raster
from flaskr.utils import get_colormap
from main import async_aggregate, async_retry
from PIL import Image, ImageCms
from io import BytesIO
import threading
import logging
import json

import rasterio
from rasterio.io import MemoryFile

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("cyan-waterbody")
logger.info("CyAN Waterbody Flask App")


@app.route('/')
def status_check():
    return "42", 200


@app.route('/waterbody/data/')
def get_data():
    args = request.args
    if "OBJECTID" in args:
        objectid = args["OBJECTID"]
    else:
        return "Missing required waterbody objectid parameter 'OBJECTID'", 200
    start_year = None
    if "start_year" in args:
        start_year = int(args["start_year"])
    start_day = None
    if "start_day" in args:
        start_day = int(args["start_day"])
    end_year = None
    if "end_year" in args:
        end_year = int(args["end_year"])
    end_day = None
    if "end_day" in args:
        end_day = int(args["end_day"])
    daily = True
    if "daily" in args:
        daily = (args["daily"] == "True")
    ranges = None
    if "ranges" in args:
        try:
            ranges = json.loads(args["ranges"])
        except Exception as e:
            logger.info("Unable to load provided ranges object, error: {}".format(e))
    geojson = None
    if "geojson" in args:
        try:
            geojson = json.loads(args["geojson"])
        except Exception as e:
            message = "Unable to load provided geojson, error: {}".format(e)
            logger.info(message)
            return message, 200
    # if geojson is not None:
    #     data = get_custom_waterbody_data(geojson=geojson, daily=daily, start_year=start_year, start_day=start_day,
    #                                      end_year=end_year, end_day=end_day, ranges=ranges)
    data = get_waterbody_data(objectid=objectid, daily=daily, start_year=start_year, start_day=start_day,
                                  end_year=end_year, end_day=end_day, ranges=ranges)
    results = {"OBJECTID": objectid, "daily": daily, "data": data}
    return results, 200


@app.route('/waterbody/search/')
def get_objectid():
    args = request.args
    lat = None
    lng = None
    gnis = None
    if "lat" in args:
        lat = float(args["lat"])
    if "lng" in args:
        lng = float(args["lng"])
    if "name" in args:
        gnis = str(args["name"])
    error = []
    if lat is None:
        error.append("Missing required latitude parameter 'lat'")
    if lng is None:
        error.append("Missing required longitude parameter 'lng'")
    if gnis is not None:
        error = []
    if len(error) > 0:
        return ", ".join(error), 200
    if gnis is not None:
        data = get_waterbody_byname(gnis_name=gnis)
        results = {"waterbodies": data if len(data) > 0 else "NA"}
    else:
        objectid, gnis = get_waterbody_bypoint(lat=lat, lng=lng)
        if objectid is not None and gnis is not None:
            data = get_waterbody_byname(gnis)
            results = {"lat": lat, "lng": lng,
                       "waterbodies": data if len(data) > 0 else "NA"}
        else:
            results = {"lat": lat, "lng": lng, "OBJECTID": int(objectid) if objectid is not None else "NA"}
    return results, 200


@app.route('/waterbody/properties/')
def get_properties():
    args = request.args
    objectid = None
    if "OBJECTID" in args:
        objectid = int(args["OBJECTID"])
    elif "objectid" in args:
        objectid = int(args["objectid"])
    else:
        return "Missing required waterbody objectid parameter 'OBJECTID'", 200
    data = get_waterbody_properties(objectid=objectid)
    result = {"objectid": objectid, "properties": data}
    return result, 200


@app.route('/waterbody/geometry/')
def get_geometry():
    args = request.args
    objectid = None
    if "OBJECTID" in args:
        objectid = int(args["OBJECTID"])
    elif "objectid" in args:
        objectid = int(args["objectid"])
    else:
        return "Missing required waterbody objectid parameter 'OBJECTID'", 200
    data = get_waterbody(objectid=objectid)
    results = {"objectid": objectid, "geojson": data}
    return results, 200


@app.route('/waterbody/image/')
def get_image():
    args = request.args
    objectid = None
    year = None
    day = None
    missing = []
    if "OBJECTID" in args:
        objectid = int(args["OBJECTID"])
    elif "objectid" in args:
        objectid = int(args["objectid"])
    else:
        missing.append("Missing required waterbody objectid parameter 'OBJECTID'")
    if "year" in args:
        year = int(args["year"])
    else:
        missing.append("Missing required year parameter 'year'")
    if "day" in args:
        day = int(args["day"])
    else:
        missing.append("Missing required day parameter 'day'")
    if len(missing) > 0:
        return ", ".join(missing), 200
    colors = {}
    use_custom = False
    if 'low' in args:
        use_custom = True
        colors['low'] = int(args['low'])
    if 'med' in args:
        use_custom = True
        colors['med'] = int(args['med'])
    if 'high' in args:
        use_custom = True
        colors['high'] = int(args['high'])
    if 'use_bin' in args:
        use_custom = True
    raster, colormap = get_waterbody_raster(objectid=objectid, year=year, day=day)
    if raster is None:
        return f"No image found for waterbody: {objectid}, year: {year}, and day: {day}", 200
    data, trans, crs, bounds = raster

    colormap[0] = (0, 0, 0, 0)
    colormap[255] = (0, 0, 0, 0)
    converted_data = [[None for i in range(data.shape[1])] for j in range(data.shape[0])]
    for y in range(0, data.shape[1]):
        for x in range(0, data.shape[0]):
            converted_data[x][y] = list(colormap[data[x][y]])

    converted_data = np.array(converted_data, dtype=np.uint8)
    png_img = Image.fromarray(converted_data, mode='RGBA')
    png_file = BytesIO()
    png_img.save(png_file, 'PNG')
    png_file.seek(0)

    # png_img.save(f"{objectid}_{year}-{day}.png", "PNG")

    response = make_response(
        send_file(
            png_file,
            as_attachment=True,
            attachment_filename=f"{objectid}_{year}-{day}.png",
            mimetype='image/png'
        )
    )
    response.headers['BBOX'] = json.dumps(bounds)
    return response

    # RETURN GEOTiFF
    # height = data.shape[0]
    # width = data.shape[1]
    # if use_custom:
    #     colormap = get_colormap(**colors)
    # profile = rasterio.profiles.DefaultGTiffProfile(count=1)
    # profile.update(transform=trans, driver='GTiff', height=height, width=width, crs=crs)
    # data = data.reshape(1, height, width)
    # with MemoryFile() as memfile:
    #     with memfile.open(**profile) as dataset:
    #         dataset.write(data)
    #         dataset.write_colormap(1, colormap)
    #     memfile.seek(0)
    #     return send_file(
    #         BytesIO(memfile.read()),
    #         as_attachment=True,
    #         attachment_filename=f"{objectid}_{year}-{day}.tiff",
    #         mimetype='image/tiff'
    #     )


@app.route('/waterbody/aggregate/')
def aggregate():
    args = request.args
    year = None
    day = None
    daily = True
    if "year" in args:
        year = int(args["year"])
    if "day" in args:
        day = int(args["day"])
    if "daily" in args:
        daily = (args["daily"] == "True")
    error = []
    if year is None:
        error.append("Missing required year parameter 'year'")
    if day is None:
        error.append("Missing required day parameter 'day'")
    if len(error) > 0:
        return ", ".join(error), 200
    th = threading.Thread(target=async_aggregate, args=(year, day, daily))
    th.start()
    return "Waterbody aggregation initiated for year: {}, day: {}, {}".format(year, day, "daily" if daily else "weekly"), 200


@app.route('/waterbody/aggregate/retry/')
def aggregate_retry():
    th = threading.Thread(target=async_retry)
    th.start()
    return "Initiated retry for dialy and weekly aggregations...", 200


if __name__ == "__main__":
    logging.info("Starting up CyAN waterbody flask app")
    app.run(debug=True, host='0.0.0.0', port=8080)
