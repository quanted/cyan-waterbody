import warnings
import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)

from flask import Flask, request, send_file, make_response, send_from_directory, g
from flaskr.db import get_waterbody_data, get_waterbody_bypoint, get_waterbody, check_status, check_overall_status, \
    check_images, get_all_states, get_all_state_counties, get_all_tribes, get_waterbody_bounds, get_waterbody_fid, get_waterbody_by_fids
from flaskr.geometry import get_waterbody_byname, get_waterbody_properties, get_waterbody_byID
from flaskr.aggregate import get_waterbody_raster
from flaskr.report import generate_report, get_report_path
from flaskr.utils import convert_cc, convert_dn
from flask_cors import CORS
from main import async_aggregate, async_retry
from PIL import Image, ImageCms
from io import BytesIO
import pandas as pd
import datetime
import threading
import logging
import json
import uuid
import time
import base64
import rasterio
from rasterio.io import MemoryFile



from celery_tasks import CeleryHandler


app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("cyan-waterbody")
logger.info("CyAN Waterbody Flask App")

cors = CORS(app, origins=["http://localhost:4200"])

celery_handler = CeleryHandler()


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


@app.route('/waterbody/data_download/')
def get_all_data():
    args = request.args
    if "OBJECTID" in args:
        objectid = args["OBJECTID"]
    else:
        return "Missing required waterbody objectid parameter 'OBJECTID'", 200
    daily = True
    if "daily" in args:
        daily = (args["daily"] == "True")

    data = get_waterbody_data(objectid=objectid, daily=daily)
    if len(data) == 0:
        return f"No data found for objectid: {objectid}", 200
    _data_df = []
    columns = ["date", "objectid"]
    for n in range(0, 256):
        if n == 0 or n > 254:
            columns.append(f"DN={n}")
        else:
            columns.append(f"CC={convert_dn(n, round=1)}")
    for d, l in data.items():
        _d = d.split(" ")
        data_row = [datetime.datetime(year=int(_d[0]), month=1, day=1) + datetime.timedelta(days=int(_d[1])), objectid]
        data_row.extend(l)
        _data_df.append(data_row)
    data_df = pd.DataFrame(_data_df, columns=columns)
    data_df.set_index('date', inplace=True)
    data_df = data_df.sort_values(by=['date'])
    response = make_response(data_df.to_csv())
    response.headers["Content-Disposition"] = f"attachment; filename={objectid}_data.csv"
    response.headers["Content-Type"] = "text/csv"
    return response


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
        objectid, fid, gnis = get_waterbody_bypoint(lat=lat, lng=lng, return_fid=True)
        if objectid is not None:
            data = get_waterbody_byID(id=objectid, fid=int(fid))
            # data = get_waterbody_byname(gnis)
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
    fid = get_waterbody_fid(objectid=objectid)
    data = get_waterbody_properties(objectid=objectid, fid=fid)

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
    # data = get_waterbody(objectid=objectid)
    fid = get_waterbody_fid(objectid=objectid)
    data = get_waterbody_by_fids(fid=fid)
    results = {"objectid": objectid, "geojson": data}
    return results, 200


@app.route('/waterbody/image/')
def get_image():
    t0 = time.time()
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
        colors['low'] = convert_cc(int(args['low']))
    if 'med' in args:
        use_custom = True
        colors['med'] = convert_cc(int(args['med']))
    if 'high' in args:
        use_custom = True
        colors['high'] = convert_cc(int(args['high']))
    if 'use_bin' in args:
        use_custom = True
    raster, colormap = get_waterbody_raster(objectid=objectid, year=year, day=day, get_bounds=False)

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

    # RETURNS IMAGE AS image/png:
    response = make_response(
        send_file(
            png_file,
            as_attachment=True,
            attachment_filename=f"{objectid}_{year}-{day}.png",
            mimetype='image/png'
        )
    )
    t1 = time.time()
    print(f"Waterbody Image Request complete, objectid: {objectid}, runtime: {round(t1-t0, 4)} sec")
    return response

    # RETURNS BASE64 IMAGE STRING AND BOUNDS AS JSON:
    # png_img.save(f"{objectid}_{year}-{day}.png", "PNG")
    # image_str = base64.b64encode(png_file.read()).decode("utf-8")
    # return {"image": image_str, "bounds": bounds}, 200

    # RETURNS GEOTiFF:
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
        return "; ".join(error), 200
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
        return "; ".join(error), 200
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
        return "; ".join(error), 200
    results = check_overall_status(start_day=s_day, start_year=s_year, end_day=e_day, end_year=e_year, daily=daily)
    return results


@app.route('/waterbody/report/')
def get_report():
    args = request.args
    county = None
    tribes = None
    objectids = None
    year = None
    day = None
    missing = []
    if "county" in args:
        county = list(int(county) for county in args["county"].split(","))
    if "tribe" in args:
        tribes = list(int(tribe) for tribe in args["tribe"].split(","))
    if "objectids" in args:
        objectids = list(int(objectid) for objectid in args["objectids"].split(","))
        # objectids = list(args["objectids"].split(","))
    if not any([county, tribes, objectids]):
        missing.append("Missing required spatial area of interest. Options include: county, tribe or objectids")
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
        colors['low'] = convert_cc(int(args['low']))
    if 'med' in args:
        use_custom = True
        colors['med'] = convert_cc(int(args['med']))
    if 'high' in args:
        use_custom = True
        colors['high'] = convert_cc(int(args['high']))
    ranges = None
    if ('high' not in args or 'med' not in args or 'low' not in args) and use_custom:
        missing.append("Missing bin categories for data. Requires high, med and low.")
    if use_custom:
        # ranges = [[[1, colors['low']], [colors['low'], colors['med']], [colors['med'], colors['high']]]]
        ranges = [[1, colors['low']], [colors['low'], colors['med']], [colors['med'], colors['high']]]
    if len(missing) > 0:
        return "; ".join(missing), 200

    request_dict = {
        'year': year,
        'day': day,
        'objectids': objectids,
        'tribes': tribes,
        'counties': county,
        'ranges': ranges,
        'report_id': str(uuid.uuid4()),
        'token': args.get('token'),
        'origin': args.get('origin'),
        'app_name': args.get('app_name')
        # 'token': g.token
    }

    # Checks that aggregation has been completed for the requests day/year/data type:
    results = check_status(day=day, year=year, daily=True)

    if results.get("status") != "COMPLETED":
        request_dict['report_id'] = None
        request_dict['status'] = False
        return request_dict, 200  # NOTE: return a non-200 code?

    # Starts report generation with celery worker:
    response = celery_handler.start_task(request_dict)

    # # Starts report generation with thread:
    # th = threading.Thread(target=generate_report, kwargs=request_dict)
    # th.start()
    
    return request_dict, 200


@app.route('/waterbody/report/status')
def report_status():
    """
    Checks status of celery task.
    """
    args = request.args
    report_id = None
    if 'report_id' in args:
        report_id = str(args["report_id"])
    if not report_id:
        return "Missing required report_id", 400
    # report_path = get_report_path(report_id=report_id)
    # if not report_path:
    #     return "Report has not completed or does not exist", 404
    # return {"status": True, "message": "Report ready to download", "report_id": report_id}
    status = celery_handler.check_celery_job_status(report_id)
    return {"report_id": report_id, "report_status": status}


@app.route('/waterbody/report/cancel')
def cancel_report():
    # NOTE: Does a cancel make sense, could use reports for other users' requests
    # NOTE: cyanweb flask should ensure the cancel is only from the user that started request
    args = request.args
    report_id = args["report_id"]
    # username = args["username"]
    celery_handler.revoke_task(report_id)


@app.route('/waterbody/report/download/')
def download_report():
    args = request.args
    report_id = None
    if 'report_id' in args:
        report_id = str(args["report_id"])
    if not report_id:
        return "Missing required report_id", 200
    report_path = get_report_path(report_id=report_id)
    if not report_path:
        return "Report has not completed or does not exist", 404
    return send_file(report_path, as_attachment=True)


@app.route('/waterbody/report_form/states/')
def get_report_states():
    states = get_all_states()
    return {"states": states}, 200


@app.route('/waterbody/report_form/counties/')
def get_report_counties():
    args = request.args
    state = None
    if "state" in args:
        state = str(args["state"])
    if not state:
        return "Missing required state argument, using STUSPS value."
    counties = get_all_state_counties(state=state)
    return {"counties": counties}, 200


@app.route('/waterbody/report_form/tribes/')
def get_report_tribes():
    tribes = get_all_tribes()
    return {"tribes": tribes}, 200


@app.route('/celery')
def test_celery():
    celery_result = celery_handler.test_celery()
    logging.warning("Celery result: {}".format(celery_result))
    return celery_result


if __name__ == "__main__":
    logging.info("Starting up CyAN waterbody flask app")
    app.run(debug=True, host='0.0.0.0', port=8080)
