import warnings

import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)

from flask import Flask, request, send_file, make_response
from flaskr.db import get_waterbody_data, get_waterbody_bypoint, get_waterbody, check_status, check_overall_status, check_images, get_waterbody_bounds
from flaskr.geometry import get_waterbody_byname, get_waterbody_properties
from flaskr.aggregate import get_waterbody_raster
from flaskr.utils import get_colormap
from flask_cors import CORS
from main import async_aggregate, async_retry
from PIL import Image, ImageCms
from io import BytesIO
import threading
import logging
import json
import base64
import rasterio
from rasterio.io import MemoryFile

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("cyan-waterbody")
logger.info("CyAN Waterbody Flask App")

cors = CORS(app, origins=["http://localhost:4200"])


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

    bounds = get_waterbody_bounds(objectid)

    data["x_min"] = bounds[1]
    data["x_max"] = bounds[2]
    data["y_min"] = bounds[3]
    data["y_max"] = bounds[4]

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
    data, trans, crs, bounds, geom = raster

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

    image_str = base64.b64encode(png_file.read()).decode("utf-8")

    return {"image": image_str, "bounds": bounds}, 200

    # RETURN IMAGE AND BBOX IN HEADER:
    # response = make_response(
    #     send_file(
    #         png_file,
    #         as_attachment=True,
    #         attachment_filename=f"{objectid}_{year}-{day}.png",
    #         mimetype='image/png'
    #     )
    # )
    # response.headers.add("Access-Control-Allow-Headers", "BBOX")
    # response.headers.add("Access-Control-Expose-Headers", "BBOX")
    # response.headers['BBOX'] = json.dumps(bounds)
    # return response

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
    if check_images(year=year, day=day, daily=daily):
        th = threading.Thread(target=async_aggregate, args=(year, day, daily))
        th.start()
        result = "Waterbody aggregation initiated for year: {}, day: {}, {}".format(year, day, "daily" if daily else "weekly"), 200
    else:
        result = "Unable to execute waterbody aggregation for year: {}, day: {}, {}, no images found".format(year, day, "daily" if daily else "weekly"), 200
    return result


@app.route('/waterbody/aggregate/retry/')
def aggregate_retry():
    th = threading.Thread(target=async_retry)
    th.start()
    return "Initiated retry for dialy and weekly aggregations...", 200


@app.route('/waterbody/aggregate/status/')
def aggregate_status():
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
    results = check_status(day=day, year=year, daily=daily)
    return results


@app.route('/waterbody/aggregate/overall_status/')
def aggregate_overall_status():
    args = request.args
    s_year = None
    s_day = None
    e_year = None
    e_day = None
    daily = True
    if "start_year" in args:
        s_year = int(args["start_year"])
    if "start_day" in args:
        s_day = int(args["start_day"])
    if "end_year" in args:
        e_year = int(args["end_year"])
    if "end_day" in args:
        e_day = int(args["end_day"])
    if "daily" in args:
        daily = (args["daily"] == "True")
    error = []
    if s_year is None:
        error.append("Missing required year parameter 'start_year'")
    if s_day is None:
        error.append("Missing required day parameter 'start_day'")
    if e_year is None:
        error.append("Missing required year parameter 'end_year'")
    if e_day is None:
        error.append("Missing required day parameter 'end_day'")
    if len(error) > 0:
        return ", ".join(error), 200
    results = check_overall_status(start_day=s_day, start_year=s_year, end_day=e_day, end_year=e_year, daily=daily)
    return results


@app.route('/waterbody/report/')
def get_report():
    args = request.args
    region = None
    county = None
    tribe = None
    state = None
    conus = False
    objectids = None
    year = None
    day = None
    missing = []
    if "region" in args:
        region = list(args["region"].split(","))
    if "conus" in args:
        conus = True
    if "county" in args:
        county = list(args["county"].split(","))
    if "tribe" in args:
        tribe = list(args["tribe"].split(","))
    if "state" in args:
        state = list(args["state"].split(","))
    if "objectids" in args:
        objectids = list(args["objectids"].split(","))
    if not any([region, conus, county, tribe, state, objectids]):
        missing.append("Missing required spatial area of interest. Options include: region, county, tribe, state, conus or objectids")
    if "year" in args:
        year = int(args["year"])
    else:
        missing.append("Missing required year parameter 'year'")
    if "day" in args:
        day = int(args["day"])
    else:
        missing.append("Missing required day parameter 'day'")
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
    if 'high' not in args or 'med' not in args or 'low' not in args:
        missing.append("Missing bin categories for data. Requires high, med and low.")
    if len(missing) > 0:
        return ", ".join(missing), 200




if __name__ == "__main__":
    logging.info("Starting up CyAN waterbody flask app")
    app.run(debug=True, host='0.0.0.0', port=8080)
