from datetime import datetime, timedelta
import pandas as pd
from flaskr.utils import convert_dn
from flaskr.geometry import get_waterbody_fids, get_waterbody_by_fids
from flaskr.db import get_waterbody_data
import time


def calculate_metrics(objectids: list, year: int, day: int, historic_days: int = 30, summary: bool = True, report: bool = False, wb_dict: dict = None):
    """
    Calculate waterbody metrics as defined in publications. Magnitude is currently commented out in the output as the publication has not yet been cleared.
    :param objectids:
    :param year:
    :param day:
    :param historic_days:
    :param summary:
    :param report:
    :param wb_dict:
    :return:
    """
    t0 = time.time()
    today = datetime.today()
    if year is None:
        year = today.year
    if day is None:
        day = today.timetuple().tm_yday
    start_date = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
    end_date = start_date - timedelta(days=historic_days)
    start_day = start_date.timetuple().tm_yday
    end_day = end_date.timetuple().tm_yday

    wb_data = []
    for oid in objectids:
        if wb_dict is not None:
            data = wb_dict
        else:
            data = get_waterbody_data(objectid=oid, start_year=end_date.year, start_day=end_day, end_year=start_date.year, end_day=start_day, ranges=None, non_blooms=True)
        for datestr, values in data.items():
            date_split = datestr.split(" ")
            data_date = datetime(year=int(date_split[0]), month=1, day=1) + timedelta(days=int(date_split[1]) - 1)
            results = {"OBJECTID": str(oid), "date": data_date}
            for i, v in enumerate(values):
                results[f"DN={i}"] = int(v)
            wb_data.append(results)
    df = pd.DataFrame(wb_data)
    detect_columns = [f"DN={i}" for i in range(1, 254)]
    all_columns = [f"DN={i}" for i in range(0, 254)]

    results = {}
    if df.shape[0] == 0:
        return results
    frequency, frequency_wb = calculate_frequency(df, detect_columns, all_columns)
    extent, extent_wb = calculate_extent(df, detect_columns, all_columns)
    magnitude_wb, area_normalized,  chia_normalized = calculate_magnitude(df, detect_columns, all_columns)
    if report:
        results["Frequency"] = frequency
        results["Extent"] = extent
        results["Frequency by Waterbody"] = frequency_wb
        results["Extent by Waterbody"] = extent_wb
        # results["Magnitude by Waterbody"] = magnitude_wb
        # results["Area Normalized Magnitude"] = area_normalized
        # results["Chia Normalized Magnitude"] = chia_normalized
        results["Metadata"] = {
            "Period": f"{historic_days} days",
            "Timestep": "daily",
            "Frequency Units": "%",
            "Extent Units": "%",
            # "Magnitude Units": "cell concentration",
            # "Area Normalized Magnitude Units": "cells/km^2",
            # "Chia Normalized Magnitude Units": "kg*km^-2"
        }
    else:
        if summary:
            results["frequency"] = frequency
            results["extent"] = extent
        results["frequency_wb"] = frequency_wb
        results["extent_wb"] = extent_wb
        # results["magnitude_wb"] = magnitude_wb
        # results["area_normalized_magnitude"] = area_normalized
        # results["chia_normalized_magnitude"] = chia_normalized
        results["metadata"] = {
            "period": f"{historic_days} days",
            "timestep": "daily",
            "frequency_units": "%",
            "extent_units": "%",
            # "magnitude_units": "cell concentration",
            # "area_normalized_magnitude_units": "cells/km^2",
            # "chia_normalized_magnitude_units": "kg*km^-2"
        }
    t1 = time.time()
    print(f"Metric calculation runtime: {round(t1 - t0, 3)} sec")
    return results


def calculate_frequency(data: pd.DataFrame, detect_columns: list, all_columns: list):
    # calculate frequency of detection, for collection of waterbody and individual waterbodies
    # spatial extent: = n of pixels with detectable CI / n of valid pixels
    # valid pixel DN=[0:253]

    detections = data[detect_columns].sum(axis=0).sum()
    all_cells = data[all_columns].sum(axis=0).sum()
    frequency = round(detections.sum() / all_cells.sum(), 4)

    wb_detections = data.groupby(by='OBJECTID')[detect_columns].sum().sum(axis=1)
    wb_all_cells = data.groupby(by='OBJECTID')[all_columns].sum().sum(axis=1)
    wb_frequency = dict(wb_detections / wb_all_cells)
    _wb_frequency = {}
    for k, v in wb_frequency.items():
        _wb_frequency[int(k)] = round(v * 100, 4)
    return frequency, _wb_frequency


def calculate_extent(data: pd.DataFrame, detect_columns: list, all_columns: list):

    detections = data[detect_columns].sum(axis=1)
    all_cells = data[all_columns].sum(axis=1)
    extent = round(100 * (detections / all_cells).mean(), 4)

    objectids = [int(oid) for oid in list(data.OBJECTID.unique())]

    oid_groups = data.groupby(by='OBJECTID')
    wb_extent = {}
    for oid in objectids:
        oid_df = oid_groups.get_group(str(oid))
        detects = oid_df[detect_columns].sum(axis=1)
        all_cells = oid_df[all_columns].sum(axis=1)
        wb_extent[oid] = round(100 * (detects / all_cells).mean(), 4)
    return extent, wb_extent


def calculate_chla(ci):
    return 6620.0 * ci


def calculate_magnitude(data: pd.DataFrame, detect_columns: list, all_columns: list):
    # bloom magnitude = 1/M * SUM(m=1->M) * 1/T * SUM(t=1->T) * SUM(p=1->P) * CI_cyano
    # bloom magnitude = 1/M * SUM(m=1->M) * 1/T * SUM(t=1->T) * (convert_dn(dn) * dn_count)
    # p: represent the number of valid pixels in a lake or waterbody
    # t: the number of composite time sequence in a season or annual study period
    # M: the number of months in a season or annual
    # Bloom Magnitude

    CI_values = [convert_dn(i) for i in range(1, 254)]
    magnitude = data[detect_columns] * CI_values
    magnitude['OBJECTID'] = data['OBJECTID']
    magnitude_wb = dict(magnitude.groupby(by='OBJECTID').sum().sum(axis=1))
    wb_magnitude_update = {}
    for k, v in magnitude_wb.items():
        wb_magnitude_update[int(k)] = round(v, 4)

    waterbody_fids = get_waterbody_fids(return_dict=True)

    # Area-normalized magnitude = bloom magnitude / lake surface area (km2)
    area_normalized_magnitude = {}
    objectids = [int(oid) for oid in list(data.OBJECTID.unique())]

    for comid in objectids:
        wb_data = get_waterbody_by_fids(waterbody_fids[comid])
        area_normalized_magnitude[int(comid)] = round(magnitude_wb[str(comid)] / wb_data[0][0]['properties']['areasqkm'], 4)

    chia_area_normalized_bloom = {}
    for k, v in area_normalized_magnitude.items():
        chia_area_normalized_bloom[int(k)] = round(595.8 * v, 4)
    return wb_magnitude_update, area_normalized_magnitude, chia_area_normalized_bloom
