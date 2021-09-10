import jinja2
from datetime import datetime, timedelta, date
from xhtml2pdf import pisa
from pathlib import Path
from flaskr.geometry import get_waterbody_properties, get_waterbody, get_county_boundary, get_state_boundary, \
    get_tribe_boundary
from flaskr.aggregate import get_waterbody_raster
from flaskr.db import get_conus_objectids, get_eparegion_objectids, get_state_objectids, get_tribe_objectids, \
    get_county_objectids, get_waterbody_data, get_group_metrics, get_county_state, get_county_geoid, \
    get_all_state_counties, get_tribe_geoid, get_state_name, get_states_from_wb
from flaskr.raster import rasterize_boundary
from flaskr.utils import DEFAULT_RANGE, get_colormap, rgb, convert_dn
import rasterio.plot
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import time
import copy
import uuid
import shutil
import os
import logging

OUTPUT_DIR = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "outputs")
STATIC_ROOT = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "static")
DISCLAIMER_PATH = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "templates", "disclaimer.txt")
REFERENCES_PATH = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "templates", "references.txt")
DEBUG = False

VERSION = "Proof of Concept Draft"
KEEP_PROPERTIES = {
    "OBJECTID": ["Object ID", int],
    "GNIS_ID": ["GNIS ID", str],
    "AREASQKM": ["Area (sqkm)", float],
    "ELEVATION": ["Elevation (m)", float],
    "STATE_ABBR": ["State", str],
    "REACHCODE": ["Reach Code", str]
}
IMAGE_FORMAT = 'png'
IMAGE_SCALE = 1


def get_env():
    template_path = os.path.join(os.path.dirname(__file__), '..', 'templates')
    j_env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=template_path),
                               autoescape=jinja2.select_autoescape())
    return j_env


def get_report_path(report_id: str):
    report_file = os.path.join(OUTPUT_DIR, f"cyanwb_report_{report_id}.pdf")
    if not os.path.isfile(report_file):
        return None
    return report_file


def generate_report(
        year: int,
        day: int,
        objectids: list = None,
        conus: bool = False,
        regions: list = None,
        states: list = None,
        tribes: list = None,
        counties: list = None,
        ranges: list = None,
        report_id: str = None
):
    t0 = time.time()
    j_env = get_env()
    title = "CyANO Waterbody Report"
    s = ""
    ranges = ranges if ranges else DEFAULT_RANGE
    for t in list((regions, states, tribes, counties)):
        if t:
            if len(t) > 1:
                s = "s"
    if report_id:
        report_id = report_id
    else:
        report_id = uuid.uuid4()
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    os.mkdir(report_root)
    waterbodies, group_type = get_waterbody_collection(objectids=objectids, conus=conus, regions=regions, states=states,
                                                       tribes=tribes, counties=counties)
    location_title = "User Selected Waterbodies" if objectids else \
        "Contiguous United States" if conus else \
            f"EPA Region{s}: " + ", ".join(regions) if regions else \
                f"State{s}: " + ",".join(states) if states else \
                    f"Tribe{s}: " + ",".join(waterbodies.keys()) if tribes else f"County: " + ",".join(
                        waterbodies.keys())
    logging.info(f"Generating new report, day: {day}, year: {year}, type: {group_type}, report_id: {report_id}")
    logging.info(f"Report: {report_id}, location: {location_title}, # of groups: {len(waterbodies)}")

    color_mapping = {
        "no detection": '#3388ff',
        "low": '#008000',
        "medium": '#c8c800',
        "high": '#ffa500',
        "very high": '#ff0000'
    }

    html = get_title(year=year, day=day, j_env=j_env, title=title, location_title=location_title)
    # html += get_toc(j_env=j_env)
    html += get_description(ranges=ranges, color_mapping=color_mapping, j_env=j_env)

    if group_type == "State":
        for k, ids in waterbodies.items():
            all_ids = get_state_objectids(states=[k], with_counties=False)
            logging.info(f"Report: {report_id}, State: {k}, # of waterbodies: {len(all_ids[k])}")
            html += get_group_block(report_id=str(report_id), year=year, day=day, group_type=group_type,
                                    group_name=get_state_name(k),
                                    objectids=all_ids[k], ranges=ranges, j_env=j_env, group_id=k,
                                    color_mapping=color_mapping, title_level=2)
            i = 1
            for county, wbs in ids.items():
                # if len(ids) > 1:
                county_geoid = get_county_geoid(county_name=county, state=k)[0]
                html += get_group_block(report_id=str(report_id), year=year, day=day, group_type="County",
                                        group_name=county,
                                        objectids=wbs, ranges=ranges, j_env=j_env, group_id=county_geoid,
                                        color_mapping=color_mapping, title_level=3)
                logging.info(
                    f"Report: {report_id}, State: {k}, County: {county}, i/n: {i}/{len(ids.keys())}, # of waterbodies: {len(wbs)}")
                wbs_html = {}
                for objectid in wbs:
                    i_html, i_name = get_waterbody_block(year=year, day=day, objectid=objectid,
                                                         report_id=str(report_id),
                                                         j_env=j_env,
                                                         ranges=ranges, title_level=4)
                    wbs_html[i_name] = i_html
                for wb in sorted(wbs_html.keys()):
                    html += wbs_html[wb]
                i += 1
            logging.info(f"Report: {report_id}, completed group: {k}")
            # Add refs
    else:
        i = 0
        for k, ids in waterbodies.items():
            group_id = uuid.uuid4()
            if group_type == "County":
                group_id = counties[i]
            elif group_type == "Tribe":
                group_id = get_tribe_geoid(k)
            html += get_group_block(report_id=str(report_id), year=year, day=day, group_type=group_type, group_name=k,
                                    objectids=ids, ranges=ranges, j_env=j_env, group_id=group_id,
                                    color_mapping=color_mapping, title_level=2)
            logging.info(f"Report: {report_id}, group: {k}, # of waterbodies: {len(ids)}")
            wbs_html = {}
            for objectid in ids:
                i_html, i_name = get_waterbody_block(year=year, day=day, objectid=objectid, report_id=str(report_id),
                                                     j_env=j_env,
                                                     ranges=ranges, title_level=3)
                wbs_html[i_name] = i_html
            for wb in sorted(wbs_html.keys()):
                html += wbs_html[wb]
            logging.info(f"Report: {report_id}, completed group: {k}")
            # Add refs
            i += 1
    html += get_references(j_env=j_env)
    html += get_closing(j_env=j_env)
    report_path = OUTPUT_DIR
    if os.path.exists(report_path):
        report_path = os.path.join(report_path, f"cyanwb_report_{report_id}.pdf")
    else:
        # report_path = os.path.join("outputs", f"cyanwb_report_{report_id}.pdf")
        os.makedirs(report_path)
    report_file = open(report_path, "w+b")
    pisa_status = pisa.CreatePDF(html, dest=report_file)
    report_file.close()
    shutil.rmtree(report_root)
    # os.rmdir(report_root)
    t1 = time.time()
    logging.info(f"Completed report, report_id: {report_id}, runtime: {round(t1 - t0, 4)} secs")
    # email report/delete report temp directory


def get_title(year: int, day: int, j_env=None, title: str = None, page_title: str = None, location_title: str = None):
    if not j_env:
        j_env = get_env()
    report_datetime = date(year=year, month=1, day=1) + timedelta(days=day - 1)
    report_date = report_datetime.strftime("%d %B %Y")
    template = j_env.get_template("report_0_title.html")
    html = template.render(
        TITLE=title,
        PAGE_TITLE=page_title,
        LOCATION_TITLE=location_title,
        DATE=report_date,
        STATIC_ROOT=f"{STATIC_ROOT}{os.sep}",
        VERSION=VERSION
    )
    return html


def get_description(ranges, color_mapping, j_env=None):
    if not j_env:
        j_env = get_env()
    template = j_env.get_template("report_2_description.html")
    levels = ["low", "medium", "high", "very high"]
    conversion_ranges = {}
    for indx, r in enumerate(ranges):
        conversion_ranges[levels[indx]] = [r[0], convert_dn(r[0]), r[1] - 1, convert_dn(r[1] - 1),
                                           color_mapping[levels[indx]]]
    conversion_ranges[levels[-1]] = [ranges[-1][1], convert_dn(ranges[-1][1]), 253, convert_dn(253),
                                     color_mapping[levels[-1]]]
    html = template.render(
        STATIC_ROOT=f"{STATIC_ROOT}{os.sep}",
        CONVERSION_TABLE=conversion_ranges,
        COLOR_MAPPING=color_mapping
    )
    return html


def get_group_block(report_id: str, year: int, day: int, group_type: str, group_name: str, objectids: list,
                    color_mapping: dict, ranges: list, j_env=None,
                    group_id: str = None, title_level: int = 2):
    if not j_env:
        j_env = get_env()
    ranges_dict = {"low": ranges[0], "medium": ranges[1], "high": ranges[2], "very high": [ranges[2][1], 254]}

    group_metrics = get_group_metrics(objectids=objectids, year=year, day=day, ranges=ranges_dict, p_days=30)
    current_color_mapping = {"no detection": copy.copy(objectids), "low": [], "medium": [], "high": [], "very high": []}
    current_key = f"{year} {day}"
    mapping_i = ["no detection"]
    for ra in ranges_dict.keys():
        ra_set = set(group_metrics[ra][current_key])
        current_color_mapping[ra] = list(ra_set)
        if len(ra_set) > 0:
            for rac in mapping_i:
                ra_set2 = current_color_mapping[rac]
                if ra != rac:
                    current_color_mapping[rac] = list(set(ra_set2) - set(ra_set))
        mapping_i.append(ra)
    week_color_mapping = {"no detection": copy.copy(objectids), "low": [], "medium": [], "high": [], "very high": []}
    mapping_i = ["no detection"]
    for ra in ranges_dict.keys():
        ra_set = []
        i = 0
        for current_key in group_metrics[ra].keys():
            if i >= 7:
                break
            ra_set = ra_set + list(set(group_metrics[ra][current_key]) - set(ra_set))
            i += 1
        week_color_mapping[ra] = list(ra_set)
        if len(ra_set) > 0:
            for rac in mapping_i:
                ra_set2 = week_color_mapping[rac]
                if ra != rac:
                    week_color_mapping[rac] = list(set(ra_set2) - set(ra_set))
        mapping_i.append(ra)

    group_state = get_county_state(county_id=int(group_id)) if group_type == "County" else None
    waterbodies_geos_raster = get_waterbody_collection_raster(groupname=group_name, grouptype=group_type,
                                                              group_id=group_id,
                                                              objectids=objectids,
                                                              current_color_mapping=current_color_mapping,
                                                              week_color_mapping=week_color_mapping,
                                                              year=year, day=day, report_id=report_id,
                                                              color_mapping=color_mapping)
    grouped_30_raster = get_collection_history30(group_metrics, object_list=objectids, color_mapping=color_mapping,
                                                 report_id=report_id, year=year, day=day, groupid=group_id)
    group_properties = {
        "Number of waterbodies": len(objectids),
        "Waterbodies with no data or no detection (current)": len(current_color_mapping["no detection"]),
        "Waterbodies with no data or no detection (previous week)": len(week_color_mapping["no detection"]),
        "Waterbodies with low cell count detection (current)": len(current_color_mapping["low"]),
        "Waterbodies with low cell count detection (previous week)": len(week_color_mapping["low"]),
        "Waterbodies with medium cell count detection (current)": len(current_color_mapping["medium"]),
        "Waterbodies with medium cell count detection (previous week)": len(week_color_mapping["medium"]),
        "Waterbodies with high cell count detection (current)": len(current_color_mapping["high"]),
        "Waterbodies with high cell count detection (previous week)": len(week_color_mapping["high"]),
        "Waterbodies with very high cell count detection (current)": len(current_color_mapping["very high"]),
        "Waterbodies with very high cell count detection (previous week)": len(week_color_mapping["very high"]),
    }
    template = j_env.get_template("report_3_group.html")
    if group_type == "User Selected Waterbodies":
        group_name = None
    html = template.render(
        GROUP_TYPE=group_type,
        GROUP_NAME=group_name,
        STATE=group_state,
        GROUP_PROPERTIES=group_properties,
        GROUP_RASTER=waterbodies_geos_raster,
        GROUP_30=grouped_30_raster,
        TITLE_LEVEL=title_level
    )
    return html


def get_waterbody_block(year: int, day: int, objectid: int, report_id: str, ranges: list, j_env=None,
                        title_level: int = 3):
    if not j_env:
        j_env = get_env()
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    waterbody_properties = get_waterbody_properties(objectid=objectid)
    waterbody_name = waterbody_properties["GNIS_NAME"]
    waterbody_properties_cleaned = {}
    wb_area = 0
    for name, value in waterbody_properties.items():
        if name in KEEP_PROPERTIES.keys():
            if value:
                waterbody_properties_cleaned[KEEP_PROPERTIES[name][0]] = KEEP_PROPERTIES[name][1](value)
                if name == "AREASQKM":
                    wb_area = KEEP_PROPERTIES[name][1](value)
            else:
                waterbody_properties_cleaned[KEEP_PROPERTIES[name][0]] = "NA"
    waterbody_raster = get_report_waterbody_raster(objectid=objectid, day=day, year=year, report_id=report_id)
    waterbody_plots = get_waterbody_plots(objectid=objectid, day=day, year=year, report_id=report_id, ranges=ranges,
                                          area=wb_area)
    report_datetime = date(year=year, month=1, day=1) + timedelta(days=day - 1)
    report_date = report_datetime.strftime("%d %B %Y")
    template = j_env.get_template("report_4_stats.html")
    html = template.render(
        WATER_BODY_NAME=waterbody_name,
        STATE=waterbody_properties["STATE_ABBR"],
        WATER_BODY_STATS=waterbody_properties_cleaned,
        REPORT_DATE=report_date,
        REPORT_ROOT=f"{report_root}{os.sep}",
        WATERBODY_RASTER=waterbody_raster,
        STATIC_ROOT=f"{STATIC_ROOT}{os.sep}",
        CURRENT_HISTOGRAM=waterbody_plots["histogram"],
        CURRENT_PIE=waterbody_plots["pie"],
        STACKED30=waterbody_plots["stacked30"],
        HISTORIC_LINE=waterbody_plots["historic"],
        TITLE_LEVEL=title_level
    )
    return html, waterbody_name


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


def get_references(j_env=None):
    if not j_env:
        j_env = get_env()
    disclaimer_text = open(DISCLAIMER_PATH, "r").readlines()
    references_text = open(REFERENCES_PATH, "r").readlines()
    references_list = []
    for reference in references_text:
        references_list.append(reference.replace("\n", "").split("; "))
    template = j_env.get_template("report_6_refs.html")
    html = template.render(
        DISCLAIMER=disclaimer_text,
        REFERENCES=references_list
    )
    return html


def get_closing(j_env=None):
    if not j_env:
        j_env = get_env()
    template = j_env.get_template("report_7_footer.html")
    html = template.render()
    return html


def get_report_waterbody_raster(objectid: int, report_id: str, day: int, year: int):
    # report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    raster_root = os.path.join(STATIC_ROOT, "raster_plots")
    image_file = f"{objectid}-{year}-{day}.png"
    image_path = os.path.join(raster_root, image_file)
    if os.path.exists(image_path):
        return image_path
    image_data, colormap = get_waterbody_raster(objectid=objectid, year=year, day=day)
    data = image_data[0]
    data = np.reshape(data, (1, data.shape[0], data.shape[1]))
    data = rasterize_boundary(image=data, boundary=image_data[4], affine=image_data[1], crs=image_data[2], value=256)[0]
    # colormap[0] = (149, 149, 149, 100)
    # colormap[254] = (159, 81, 44, 100)
    # colormap[255] = (0, 0, 0, 100)
    colormap[256] = (0, 0, 0, 0)
    mapped_image = [[None for i in range(data.shape[1])] for j in range(data.shape[0])]
    for y in range(0, data.shape[1]):
        for x in range(0, data.shape[0]):
            mapped_image[x][y] = list(colormap[data[x][y]])
    converted_data = np.array(mapped_image, dtype=np.uint8)
    fig, ax = plt.subplots()
    fig.suptitle(f'Satellite Imagery for Waterbody', fontsize=12)
    raster_data = rasterio.plot.reshape_as_raster(converted_data)
    rasterio.plot.show(raster_data, transform=image_data[1], ax=ax)
    boundary = image_data[4].to_crs(image_data[2])
    boundary.plot(ax=ax, facecolor='none', edgecolor='#3388ff', linewidth=2)
    plt.axis('off')
    plt.savefig(image_path)
    return image_path


def get_waterbody_collection_raster(groupname: str, grouptype: str, group_id: str, objectids: list,
                                    current_color_mapping: dict, week_color_mapping, year: int, day: int,
                                    report_id: str,
                                    color_mapping):
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    image_file = f"{grouptype}-{group_id}_{year}_{day}.png"
    image_path = os.path.join(report_root, image_file)

    if os.path.exists(image_path):
        return image_path
    current_date = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
    fig, ax = plt.subplots(1, 2)
    ax1 = ax[0]
    ax2 = ax[1]
    # if grouptype == "County":
    fig.suptitle(f'{groupname} Waterbody Max Occurrence', fontsize=12)
    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    ax1.set_xlabel(f"Current Max Occurrence")
    ax2.set_xlabel(f"Previous Week Max Occurrence")

    if grouptype == "County":
        county_geo, county_crs = get_county_boundary(geoid=str(group_id))
        if county_geo["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in county_geo["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            county_poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=county_crs)
        else:
            county_poly = gpd.GeoSeries(Polygon(county_geo["geometry"]["coordinates"][0]), crs=county_crs)
        county_poly.plot(ax=ax1, edgecolor='#a8a79b', color='#a8a79b')
        county_poly.plot(ax=ax2, edgecolor='#a8a79b', color='#a8a79b')
    elif grouptype == "Tribe":
        tribe_geo, tribe_crs = get_tribe_boundary(tribe=str(group_id))
        if tribe_geo["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in tribe_geo["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            tribe_poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=tribe_crs)
        else:
            tribe_poly = gpd.GeoSeries(Polygon(tribe_geo["geometry"]["coordinates"][0]), crs=tribe_crs)
        tribe_poly.plot(ax=ax1, edgecolor='#a8a79b', color='#a8a79b')
        tribe_poly.plot(ax=ax2, edgecolor='#a8a79b', color='#a8a79b')
    elif grouptype == "State":
        state_geo, state_crs = get_state_boundary(state=str(group_id))
        if state_geo["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in state_geo["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            state_poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=state_crs)
        else:
            state_poly = gpd.GeoSeries(Polygon(state_geo["geometry"]["coordinates"][0]), crs=state_crs)
        state_poly.plot(ax=ax1, edgecolor='#a8a79b', color='#a8a79b')
        state_poly.plot(ax=ax2, edgecolor='#a8a79b', color='#a8a79b')
    else:
        states = get_states_from_wb(tuple(objectids))
        for state in states:
            state_geo, state_crs = get_state_boundary(state=str(state))
            if state_geo["geometry"]["type"] == "MultiPolygon":
                poly_geos = []
                for p in state_geo["geometry"]["coordinates"]:
                    poly_geos.append(Polygon(p[0]))
                state_poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=state_crs)
            else:
                state_poly = gpd.GeoSeries(Polygon(state_geo["geometry"]["coordinates"][0]), crs=state_crs)
            state_poly.plot(ax=ax1, edgecolor='#a8a79b', color='#a8a79b')
            state_poly.plot(ax=ax2, edgecolor='#a8a79b', color='#a8a79b')
        fig.suptitle(f'Waterbody Max Occurrence', fontsize=12)

    boundaries, crs = get_waterbody(objectids=objectids)
    for boundary in boundaries:
        if boundary["geometry"]["type"] == "MultiPolygon":
            poly_geos = []
            for p in boundary["geometry"]["coordinates"]:
                poly_geos.append(Polygon(p[0]))
            poly = gpd.GeoSeries(MultiPolygon(poly_geos), crs=crs)
        else:
            poly = gpd.GeoSeries(Polygon(boundary["geometry"]["coordinates"][0]), crs=crs)
        objectid = int(boundary["properties"]["OBJECTID"])
        current_color = color_mapping['no detection']
        for current_rating, wbs in current_color_mapping.items():
            if objectid in wbs:
                current_color = color_mapping[current_rating]
                break
        week_color = color_mapping['no detection']
        for week_rating, wbs in week_color_mapping.items():
            if objectid in wbs:
                week_color = color_mapping[week_rating]
        poly.plot(ax=ax1, facecolor='none', color=current_color, linewidth=1)
        poly.plot(ax=ax2, facecolor='none', color=week_color, linewidth=1)
    # plt.axis('off')
    # plt.show()
    plt.tight_layout()
    plt.savefig(image_path, dpi=140)
    plt.close('all')
    return image_path


def get_collection_history30(stacked_data, object_list, color_mapping, report_id, year: int, day: int, groupid):
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    x_dates = []
    y_values = {"no detection": [], "low": [], "medium": [], "high": [],
                "very high": []}
    current_date0 = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
    for i in range(0, 30):
        current_date = current_date0 - timedelta(days=i)
        current_date_value = f"{current_date.year}-{current_date.month}-{current_date.day}"
        x_dates.append(current_date_value)
        current_key = f"{current_date.year} {current_date.timetuple().tm_yday}"
        historic_mapping = {"no detection": copy.copy(object_list), "low": [], "medium": [], "high": [],
                            "very high": []}
        mapping_i = ["no detection"]

        for ra in historic_mapping.keys():
            if ra not in stacked_data.keys():
                continue
            ra_set = set(stacked_data[ra][current_key])
            historic_mapping[ra] = list(ra_set)
            if len(ra_set) > 0:
                for rac in mapping_i:
                    ra_set2 = historic_mapping[rac]
                    if ra != rac:
                        historic_mapping[rac] = list(set(ra_set2) - set(ra_set))
            mapping_i.append(ra)
        for k, v in historic_mapping.items():
            # y_values[k].append(len(v))
            y_values[k].append(round(len(v) / len(object_list), 2))
    columns = ["no detection", "low", "medium", "high", "very high"]
    traces = []
    for c in columns:
        traces.append(go.Scatter(
            x=x_dates, y=y_values[c], name=c, marker_color=color_mapping[c], mode='lines', fill='tozeroy'))
    history_30_fig = go.Figure(data=traces)
    history_30_fig.update_layout(title={"text": "Percentage of Waterbodies with Cyano by Max Occurrence",
                                        'y': 0.9, 'x': 0.5,
                                        'xanchor': 'center', 'yanchor': 'top',
                                        'font': {'size': 26}},
                                 xaxis_title="Date", yaxis_title="Percent by Category",
                                 width=1200, height=600
                                 )
    # history_30_fig.show()
    history_30 = f"{groupid}-{year}-{day}-history30.{IMAGE_FORMAT}"
    history_30_path = os.path.join(report_root, history_30)
    history_30_fig.write_image(history_30_path, scale=IMAGE_SCALE, format=IMAGE_FORMAT)
    plt.close('all')
    return history_30_path


def get_waterbody_plots(objectid: int, report_id: str, day: int, year: int, ranges: list, area: float):
    report_root = os.path.join(STATIC_ROOT, "temp", str(report_id))
    start_day = datetime(year=year, month=1, day=1) + timedelta(days=day - 31)
    color_mapping = {
        "Below Detection": 'rgb(128, 128, 128)',
        "Low": 'rgb(0, 128, 0)',
        "Medium": 'rgb(200, 200, 0)',
        "High": 'rgb(255, 165, 0)',
        "Very High": 'rgb(255, 0, 0)',
        "Land": 'rgb(125, 75, 0)',
        "No Data": 'rgb(0, 0, 0)'
    }
    plots = {}
    ranges0 = copy.copy(ranges)
    data = get_waterbody_data(objectid=str(objectid), daily=True, end_year=year, end_day=day,
                              start_year=start_day.year, start_day=start_day.timetuple().tm_yday)
    ranged_data = get_waterbody_data(objectid=str(objectid), daily=True, end_year=year, end_day=day, ranges=ranges0,
                                     non_blooms=True)
    plots["histogram"] = get_waterbody_histogram(data=data, report_root=report_root, objectid=objectid,
                                                 day=day, year=year, ranges=ranges0)
    plots["pie"] = get_waterbody_pie(data=data, ranged_data=ranged_data, report_root=report_root, objectid=objectid,
                                     day=day, year=year, color_mapping=color_mapping)
    plots["stacked30"] = get_waterbody_30bar(ranged_data=ranged_data, report_root=report_root, objectid=objectid,
                                             day=day, year=year, area=area, color_mapping=color_mapping)
    plots["historic"] = get_waterbody_history(ranged_data=ranged_data, report_root=report_root, objectid=objectid,
                                              year=year, day=day, color_mapping=color_mapping)
    del data
    del ranged_data
    return plots


def get_waterbody_histogram(data, report_root, objectid: int, day: int, year: int, ranges: list):
    # -------- current day histogram ------- #
    current_key = f"{year} {day}"
    current_data = copy.copy(data[current_key])
    current_data[0] = 0.0
    current_data[-1] = 0.0
    current_data[-2] = 0.0
    current_fig = make_subplots(
        rows=2, cols=1,
        vertical_spacing=0.1,
        specs=[[{"type": "scatter"}], [{"type": "table"}]]
    )
    # cell_ranges = np.around(np.power(10, (3 / 250) * np.array(np.arange(0, 256)) - 4.2) * 10 ** 8, 2)
    cell_ranges = np.around(convert_dn(np.array(np.arange(0, 256))), 2)
    min_i = np.where(np.array(data[current_key][0:254]) > 0)[0]
    min_i0 = 1 if len(min_i) > 1 else 0
    # min_con = np.power(10, (3 / 250) * min_i[min_i0] - 4.2) * 10 ** 8
    min_con = convert_dn(min_i[min_i0])
    max_i = np.where(np.array(data[current_key][0:254]) > 0)[0]
    # max_con = np.power(10, (3 / 250) * max_i[-1] - 4.2) * 10 ** 8
    max_con = convert_dn(max_i[-1])
    mean_total = 0
    mean_count = 0
    i = 0
    for c in current_data:
        mean_total += c * i
        mean_count += c
        i += 1
    mean_actual = int(mean_total / mean_count) if mean_count > 0 else 0
    # mean_con = np.power(10, (3 / 250) * mean_actual - 4.2) * 10 ** 8
    mean_con = convert_dn(mean_actual)
    std_term = 0
    for c in current_data:
        if c > 0:
            std_term += (c - mean_actual) ** 2
    std_actual = int(np.sqrt(1 / 253 * std_term))
    # std_con = np.power(10, (3 / 250) * std_actual - 4.2) * 10 ** 8
    std_con = convert_dn(std_actual)
    day_metrics_names = ["Min (cells/mL)", "Max (cells/mL)", "Average (cells/mL)", "Standard Deviation (cells/mL)"]
    day_metrics_values = [round(min_con, 2), round(max_con, 2), round(mean_con, 2), round(std_con, 2)]
    range_blocks_x = {
        "low": cell_ranges[ranges[0][0]:ranges[0][1]],
        "medium": cell_ranges[ranges[1][0]:ranges[1][1]],
        "high": cell_ranges[ranges[2][0]:ranges[2][1]],
        "very high": cell_ranges[ranges[2][1]:254],
    }
    range_blocks_y = {
        "low": current_data[ranges[0][0]:ranges[0][1]],
        "medium": current_data[ranges[1][0]:ranges[1][1]],
        "high": current_data[ranges[2][0]:ranges[2][1]],
        "very high": current_data[ranges[2][1]:254]
    }
    x_tick_value = [int(ranges[0][0] + 10), ranges[0][1], ranges[1][1], ranges[2][1], int((254 + ranges[2][1]) / 2),
                    254]
    x_tick_text = [f"{int(cell_ranges[int(ranges[0][0] + 10)] / 1000)}K", f"{int(cell_ranges[ranges[0][1]] / 1000)}K",
                   f"{int(cell_ranges[ranges[1][1]] / 1000)}K", f"{int(cell_ranges[ranges[2][1]] / 1000)}K",
                   f"{int(cell_ranges[int((253 + ranges[2][1]) / 2)] / 1000)}K", f"{int(cell_ranges[254] / 1000)}K"]
    traces = [go.Bar(
        x=np.arange(ranges[0][0], ranges[0][1]),
        y=range_blocks_y["low"],
        name="low",
        marker_color=f'rgb{rgb["low"]}'
    ),
        go.Bar(
            x=np.arange(ranges[1][0], ranges[1][1]),
            y=range_blocks_y["medium"],
            name="medium",
            marker_color=f'rgb{rgb["medium"]}'
        ),
        go.Bar(
            x=np.arange(ranges[2][0], ranges[2][1]),
            y=range_blocks_y["high"],
            name="high",
            marker_color=f'rgb{rgb["high"]}'
        ),
        go.Bar(
            x=np.arange(ranges[2][1] + 1, 254),
            y=range_blocks_y["very high"],
            name="very high",
            marker_color=f'rgb{rgb["vhigh"]}'
        )
    ]
    layout = {
        'barmode': 'relative',
        'yaxis': {'tickformat': 'd'},
        'font': {'size': 22},
        'autosize': False,
        'width': 1200,
        'height': 600,
    }
    for trace in traces:
        current_fig.add_trace(trace, row=1, col=1)
    current_fig.add_trace(go.Table(
        header=dict(values=day_metrics_names, font=dict(size=18), align='center'),
        cells=dict(values=day_metrics_values, align='center', font=dict(size=18), height=30)), row=2, col=1
    )
    current_fig.update_layout(**layout, yaxis_title="Pixel Count",
                              title={"text": "Cell Concentration Histogram (cell/mL)",
                                     'y': 0.9, 'x': 0.5,
                                     'xanchor': 'center', 'yanchor': 'top'}, )
    current_fig.update_xaxes(tickvals=x_tick_value, ticktext=x_tick_text)
    # current_fig.show()
    current_fig_file = f"{objectid}-{year}-{day}-histogram.{IMAGE_FORMAT}"
    current_fig_path = os.path.join(report_root, current_fig_file)
    current_fig.write_image(current_fig_path, scale=IMAGE_SCALE, format=IMAGE_FORMAT)
    plt.close('all')
    del current_data
    return current_fig_path


def get_waterbody_pie(data, ranged_data, report_root, objectid: int, day: int, year: int, color_mapping: dict):
    current_key = f"{year} {day}"
    current_data = copy.copy(ranged_data[current_key])
    temp_current_data = []
    for i in range(4):
        temp_current_data.append(current_data[i])
    current_data = temp_current_data
    pie_names = ["Below Detection", "Low", "Medium", "High", "Very High", "Land", "No Data"]

    pie_data = [[pie_names[0], int(data[current_key][0])]]
    i = 1
    for d in current_data:
        pie_data.append([pie_names[i], d])
        i += 1
    pie_data.append([pie_names[-2], int(data[current_key][254])])
    pie_data.append([pie_names[-1], int(data[current_key][255])])
    pie_df = pd.DataFrame(pie_data, columns=["level", "value"])
    pie_fig = px.pie(pie_df, values="value", names="level", color="level", color_discrete_map=color_mapping)
    pie_fig.update_traces(textposition='inside', textinfo='percent+label')
    pie_fig.update_layout(title={"text": "Percentage by Category",
                                 'y': 0.99, 'x': 0.5},
                          font={'size': 22}, width=1200, height=600)
    # pie_fig.show()
    current_pie_file = f"{objectid}-{year}-{day}-pie.{IMAGE_FORMAT}"
    current_pie_path = os.path.join(report_root, current_pie_file)
    pie_fig.write_image(current_pie_path, scale=IMAGE_SCALE, format=IMAGE_FORMAT)
    plt.close('all')
    del current_data
    return current_pie_path


def get_waterbody_30bar(ranged_data, report_root, objectid: int, day: int, year: int, area: float, color_mapping):
    current_date = datetime(year=year, month=1, day=1) + timedelta(days=day - 31)
    all_columns = ["Date", "Low (sqkm)", "Medium (sqkm)", "High (sqkm)", "Very High (sqkm)", "Below Detection (sqkm)",
                   "Land (sqkm)", "No Data (sqkm)", f"Pixel Area<br>(sqkm)", "Geometry<br>Area<br>(sqkm)",
                   "Low (%)", "Medium (%)", "High (%)", "Very High (%)", "Below Detection (%)", "Land (%)",
                   "No Data (%)"]
    bold_columns = []
    for c in all_columns:
        bold_columns.append(f"<b>{c}</b>")
    all_columns = bold_columns
    columns = ["Low", "Medium", "High", "Very High"]
    ranged_data = copy.copy(ranged_data)
    x_dates = []
    stacked_data = {"Low": [], "Medium": [], "High": [], "Very High": [], "Below Detection": [], "Land": [],
                    "No Data": []}
    stacked_csv = []
    current_date0 = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
    for i in range(0, 30):
        current_date = current_date0 - timedelta(days=i)
        i_k = f"{current_date.year} {current_date.timetuple().tm_yday}"
        if i_k in ranged_data.keys():
            x_dates.append(f"{current_date.year}-{current_date.month}-{current_date.day}")
            stacked_data["Low"].append(ranged_data[i_k][0])
            stacked_data["Medium"].append(ranged_data[i_k][1])
            stacked_data["High"].append(ranged_data[i_k][2])
            stacked_data["Very High"].append(ranged_data[i_k][3])
            stacked_data["Below Detection"].append(ranged_data[i_k][4])
            stacked_data["Land"].append(ranged_data[i_k][5])
            stacked_data["No Data"].append(ranged_data[i_k][6])
            percentages = np.around(100 * (np.divide(ranged_data[i_k], int(np.sum(ranged_data[i_k])))), 2)
            stacked_csv.append(
                [f"{current_date.year}-{current_date.month}-{current_date.day}",
                 *np.around(0.09 * np.array(ranged_data[i_k]), 4),
                 np.around(0.09 * np.sum(ranged_data[i_k]), 4), round(area, 2), *percentages])
        # current_date = current_date + timedelta(days=1)
    stacked_csv = [*zip(*stacked_csv)]  # transposing 2d matrix
    stacked_30_fig = make_subplots(
        rows=2, cols=1,
        vertical_spacing=0.05,
        specs=[[{"type": "scatter"}], [{"type": "table"}]]
    )
    for c in columns:
        stacked_30_fig.add_trace(go.Bar(
            x=x_dates, y=stacked_data[c], name=c, marker_color=color_mapping[c], text=stacked_data[c],
            textposition='inside', textangle=0), row=1, col=1)
    stacked_30_fig.add_trace(go.Table(
        header=dict(values=all_columns, font=dict(size=12), align='center'),
        cells=dict(values=stacked_csv, align='center', font=dict(size=12))), row=2, col=1
    )
    stacked_30_fig.update_layout(title={"text": "30 Day Waterbody History",
                                        'y': 0.98, 'x': 0.5,
                                        'xanchor': 'center', 'yanchor': 'top'},
                                 yaxis_title="Cell Count", font={'size': 22},
                                 width=1600, height=1600)
    # stacked_30_fig.show()
    stacked_30_file = f"{objectid}-{year}-{day}-stacked30.{IMAGE_FORMAT}"
    stacked_30_path = os.path.join(report_root, stacked_30_file)
    stacked_30_fig.write_image(stacked_30_path, scale=IMAGE_SCALE, format=IMAGE_FORMAT)
    del stacked_csv
    plt.close('all')
    return stacked_30_path


def get_waterbody_history(ranged_data, report_root, objectid: int, year: int, day: int, color_mapping):
    columns = ["Low", "Medium", "High", "Very High"]
    x_dates = []
    stacked_data = {"Low": [], "Medium": [], "High": [], "Very High": []}
    for k, r in ranged_data.items():
        yd = k.split(" ")
        dk = datetime(year=int(yd[0]), month=1, day=1) + timedelta(days=int(yd[1]) - 1)
        x_dates.append(f"{dk.year}-{dk.month}-{dk.day}")
        stacked_data["Low"].append(r[0])
        stacked_data["Medium"].append(r[1])
        stacked_data["High"].append(r[2])
        stacked_data["Very High"].append(r[3])
    historic_line_fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.06,
        specs=[[{"type": "scatter"}], [{"type": "scatter"}], [{"type": "scatter"}], [{"type": "scatter"}]],
        y_title="Cell Count", x_title="Date"
    )
    i = 1
    for c in columns:
        historic_line_fig.add_trace(go.Scatter(x=x_dates, y=stacked_data[c], mode='lines', name=c,
                                               line=dict(color=color_mapping[c], width=1), fill='tozeroy',
                                               connectgaps=True), row=i, col=1)
        i += 1
    historic_line_fig.update_layout(title={"text": "Complete Waterbody History",
                                           'y': 0.9, 'x': 0.5,
                                           'xanchor': 'center', 'yanchor': 'top',
                                           'font': {'size': 22}}, width=1200, height=600, font={'size': 22})
    historic_line_fig.layout.annotations[0]["font"] = {'size': 22}
    historic_line_fig.layout.annotations[1]["font"] = {'size': 22}
    # historic_line_fig.show()
    historic_line_file = f"{objectid}-{year}-{day}-historic.{IMAGE_FORMAT}"
    historic_line_path = os.path.join(report_root, historic_line_file)
    historic_line_fig.write_image(historic_line_path, scale=IMAGE_SCALE, format=IMAGE_FORMAT)
    plt.close('all')
    del stacked_data
    return historic_line_path


def get_waterbody_collection(
        objectids: list = None,
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
        wb_type = "CONUS"
    elif regions:
        wb_collection = get_eparegion_objectids(regions)
        wb_type = "EPA Region"
    elif states:
        wb_collection = get_state_objectids(states)
        wb_type = "State"
    elif tribes:
        wb_collection = get_tribe_objectids(tribes)
        wb_type = "Tribe"
    elif counties:
        wb_collection = get_county_objectids(counties)
        wb_type = "County"
    else:
        wb_collection["user_selected"] = objectids if objectids else []
        wb_type = "User Selected Waterbodies"
    return wb_collection, wb_type


if __name__ == "__main__":
    import time

    t0 = time.time()
    year = 2021
    day = 244
    # states = ["Georgia"]
    objectids = [6624886, 7561665, 862709, 115083, 476621]
    states = ["WY"]
    county = ['13067', '12093']
    tribe = ['5550']
    # county = ['13049', '13067']
    # generate_report(year=year, day=day, objectids=objectids)
    # generate_report(year=year, day=day, counties=county)
    # generate_report(year=year, day=day, tribes=tribe)
    generate_report(year=year, day=day, states=states)
    t1 = time.time()
    print(f"Completed report, runtime: {t1 - t0} sec")
