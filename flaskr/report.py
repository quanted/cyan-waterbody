import jinja2
from datetime import datetime, timedelta, date
from xhtml2pdf import pisa
from pathlib import Path
from PIL import Image
from flaskr.geometry import get_waterbody_properties
from flaskr.aggregate import get_waterbody_raster
from flaskr.db import get_conus_objectids, get_eparegion_objectids, get_state_objectids, get_tribe_objectids, get_county_objectids
from flaskr.raster import rasterize_boundary
import rasterio.plot
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import uuid
import os

STATIC_ROOT = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "static")
DISCLAIMER_PATH = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "templates", "disclaimer.txt")
DEBUG = True
VERSION = "Proof of Concept Draft"
KEEP_PROPERTIES = {
    "OBJECTID": ["Object ID", int],
    "GNIS_ID": ["GNIS ID", str],
    "AREASQKM": ["Area (sqkm)", float],
    "ELEVATION": ["Elevation (m)", float],
    "STATE_ABBR": ["State", str],
    "REACHCODE": ["Reach Code", str]
}


def get_env():
    template_path = os.path.join(os.path.dirname(__file__), '..', 'templates')
    j_env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=template_path), autoescape=jinja2.select_autoescape())
    return j_env


def generate_report(
        year: int,
        day: int,
        objectids: list = None,
        conus: bool = False,
        regions: list = None,
        states: list = None,
        tribes: list = None,
        counties: list = None):

    j_env = get_env()
    title = "CyANO Waterbody Report"
    s = ""
    for t in list((regions, states, tribes, counties)):
        if t:
            if len(t) > 1:
                s = "s"
    location_title = "User Selected Waterbodies" if objectids else \
        "Contiguous United States" if conus else \
        f"EPA Region{s}: " + ", ".join(regions) if regions else \
        f"State{s}: " + ",".join(states) if states else \
        f"Tribe{s}: " + ",".join(tribes) if tribes else f"County: " + ",".join(counties)
    report_id = uuid.uuid4()
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    os.mkdir(report_root)
    waterbodies = get_waterbody_collection(objectids=objectids, conus=conus, regions=regions, states=states, tribes=tribes, counties=counties)
    html = get_title(year=year, day=day, j_env=j_env, title=title, location_title=location_title)
    html += get_toc(j_env=j_env)
    for k, ids in waterbodies.items():
        # if conus run summary over all waterbodies, then do a summary for each state
        # summary description for area k, for all ids
        # if len(ids) > 1: run summary stats on all ids
        for objectid in ids:
            html += get_waterbody_block(year=year, day=day, objectid=objectid, report_id=str(report_id), j_env=j_env)
        # html += get_groupend(j_env=j_env)
    html += get_closing(j_env=j_env)
    report_file = open(os.path.join("..", "outputs", f"test_report_{year}_{day}_{report_id}.pdf"), "w+b")
    pisa_status = pisa.CreatePDF(html, dest=report_file)
    report_file.close()
    # email report/delete report temp directory


def get_title(year: int, day: int, j_env=None, title: str = None, page_title: str = None, location_title: str = None):
    if not j_env:
        j_env = get_env()
    report_datetime = date(year=year, month=1, day=1) + timedelta(days=day - 1)
    report_date = report_datetime.strftime("%d %B %Y")
    disclaimer_text = open(DISCLAIMER_PATH, "r").readlines()
    template = j_env.get_template("report_0_title.html")
    html = template.render(
        TITLE=title,
        PAGE_TITLE=page_title,
        LOCATION_TITLE=location_title,
        DATE=report_date,
        STATIC_ROOT=f"{STATIC_ROOT}{os.sep}",
        DISCLAIMER=disclaimer_text,
        VERSION=VERSION
    )
    return html


def get_waterbody_block(year: int, day: int, objectid:int, report_id:str, j_env=None):
    if not j_env:
        j_env = get_env()
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    waterbody_properties = get_waterbody_properties(objectid=objectid)
    waterbody_name = waterbody_properties["GNIS_NAME"]
    waterbody_properties_cleaned = {}
    for name, value in waterbody_properties.items():
        if name in KEEP_PROPERTIES.keys():
            waterbody_properties_cleaned[KEEP_PROPERTIES[name][0]] = KEEP_PROPERTIES[name][1](value)
    waterbody_raster = get_report_waterbody_raster(objectid=objectid, day=day, year=year, report_id=report_id)
    report_datetime = date(year=year, month=1, day=1) + timedelta(days=day - 1)
    report_date = report_datetime.strftime("%d %B %Y")
    template = j_env.get_template("report_4_stats.html")
    html = template.render(
        WATER_BODY_NAME=waterbody_name,
        WATER_BODY_STATS=waterbody_properties_cleaned,
        REPORT_DATE=report_date,
        REPORT_ROOT=f"{report_root}{os.sep}",
        WATERBODY_RASTER=waterbody_raster
    )
    return html


def get_toc(j_env=None):
    if not j_env:
        j_env = get_env()
    template = j_env.get_template("report_1_index.html")
    html = template.render()
    return html


def get_groupend(j_env=None):
    if not j_env:
        j_env = get_env()
    template = j_env.get_template("report_5_groupend.html")
    html = template.render()
    return html


def get_closing(j_env=None):
    if not j_env:
        j_env = get_env()
    template = j_env.get_template("report_7_footer.html")
    html = template.render()
    return html


def get_report_waterbody_raster(objectid: int, report_id: str, day: int, year: int):
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    image_file = f"{objectid}-{year}-{day}.png"
    image_path = os.path.join(report_root, image_file)
    image_data, colormap = get_waterbody_raster(objectid=objectid, year=year, day=day)
    data = image_data[0]
    # data[data == 255] = 0
    # data = rasterize_boundary(image=data, boundary=image_data[4], affine=image_data[1], crs=image_data[2], value=256)

    # colormap[0] = (0, 0, 0, 0)
    # colormap[255] = (0, 0, 0, 0)
    # colormap[256] = (51, 136, 255, 255)
    mapped_image = [[None for i in range(data.shape[1])] for j in range(data.shape[0])]
    for y in range(0, data.shape[1]):
        for x in range(0, data.shape[0]):
            mapped_image[x][y] = list(colormap[data[x][y]])
    converted_data = np.array(mapped_image, dtype=np.uint8)
    fig, ax = plt.subplots()
    raster_data = rasterio.plot.reshape_as_raster(converted_data)
    rasterio.plot.show(raster_data, transform=image_data[1], ax=ax)
    boundary = image_data[4].to_crs(image_data[2])
    boundary.plot(ax=ax, facecolor='none', edgecolor='#3388ff', linewidth=1)
    plt.axis('off')
    # plt.show()
    plt.savefig(image_path)
    # png_img = Image.fromarray(converted_data, mode='RGBA')
    # png_img.save(image_path, 'PNG')
    return image_path


def get_waterbody_collection(
        objectids:list = None,
        conus: bool = False,
        regions: list = None,
        states: list = None,
        tribes: list = None,
        counties: list = None
):
    wb_collection = {}
    if DEBUG:
        return {"user_selected": [1438115]}
    if conus:
        wb_collection = get_conus_objectids()
    elif regions:
        wb_collection = get_eparegion_objectids(regions)
    elif states:
        wb_collection = get_state_objectids(states)
    elif tribes:
        wb_collection = get_tribe_objectids(tribes)
    elif counties:
        wb_collection = get_county_objectids(counties)
    else:
        wb_collection["user_selected"] = objectids if objectids else []
    return wb_collection


if __name__ == "__main__":
    year = 2021
    day = 234
    states = ["Georgia"]
    generate_report(year=year, day=day, states=states)
