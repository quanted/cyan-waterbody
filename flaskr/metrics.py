from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from flaskr.utils import convert_dn
from flaskr.geometry import get_waterbody_fids, get_waterbody_by_fids
from flaskr.db import get_waterbody_data
import time


def get_data(objectids, start_date: datetime, end_date: datetime, wb_dict: dict = None):
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
    return df


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
    df = get_data(objectids=objectids, start_date=start_date, end_date=end_date, wb_dict=wb_dict)
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
        results["Chia Normalized Magnitude"] = chia_normalized
        results["Metadata"] = {
            "Period": f"{historic_days} days",
            "Timestep": "daily",
            "Frequency Units": "%",
            "Extent Units": "%",
            # "Magnitude Units": "cell concentration",
            # "Area Normalized Magnitude Units": "cells/km^2",
            "Chia Normalized Magnitude Units": "kg*km^-2"
        }
    else:
        if summary:
            results["frequency"] = frequency
            results["extent"] = extent
        results["frequency_wb"] = frequency_wb
        results["extent_wb"] = extent_wb
        # results["magnitude_wb"] = magnitude_wb
        # results["area_normalized_magnitude"] = area_normalized
        results["chia_normalized_magnitude"] = chia_normalized
        results["metadata"] = {
            "period": f"{historic_days} days",
            "timestep": "daily",
            "frequency_units": "%",
            "extent_units": "%",
            # "magnitude_units": "cell concentration",
            # "area_normalized_magnitude_units": "cells/km^2",
            "chia_normalized_magnitude_units": "kg*km^-2"
        }
    t1 = time.time()
    print(f"Metric calculation runtime: {round(t1 - t0, 3)} sec")
    return results


def calculate_frequency(data: pd.DataFrame, detect_columns: list, all_columns: list):
    """
    For the timespan of observations, the average lake-scale bloom frequencies are computed by averaging the
    pixel-scale bloom frequencies for all pixels contained with a lake.
    :param data:
    :param detect_columns: The list of column names which correspond to detection DN, DN=1:253
    :param all_columns: The entire list of valid pixel columns, DN=0-254
    :return:
    """
    # detect
    # valid pixel DN=[0:253]

    # For all the dates in the range,
    # detections = data[detect_columns].sum(axis=0).sum()
    # all_cells = data[all_columns].sum(axis=0).sum()
    # frequency = round(detections.sum() / all_cells.sum(), 4)

    # For all dates in the timespan, how many dates was there any detection.
    # That count is divided by the total number of days.
    detections0 = np.count_nonzero(data[detect_columns].sum(axis=1).to_numpy())
    all_cells0 = data[all_columns].sum(axis=1).size
    frequency = round(100 * (detections0 / all_cells0), 2)

    # wb_detections = data.groupby(by='OBJECTID')[detect_columns].sum().sum(axis=1)
    # wb_all_cells = data.groupby(by='OBJECTID')[all_columns].sum().sum(axis=1)
    # wb_frequency = dict(wb_detections / wb_all_cells)

    # _wb_frequency = {}
    # for k, v in wb_frequency.items():
    #     _wb_frequency[int(k)] = round(v * 100, 2)

    _wb_frequency = {}
    for object_id, y in data.groupby(by='OBJECTID')[detect_columns]:
        _wb_frequency[int(object_id)] = round(100 * (np.count_nonzero(y.sum(axis=1).to_numpy())/y.shape[0]), 2)

    return frequency, _wb_frequency


def calculate_extent(data: pd.DataFrame, detect_columns: list, all_columns: list):

    # Extent is the average extent of detections over the timespan.
    # Calculated by the average of (# of detection pixels on that date)/(total # of pixels) over the timespan.
    detections = data[detect_columns].sum(axis=1)
    all_cells = data[all_columns].sum(axis=1)
    extent_i = (detections / all_cells).to_numpy()
    extent = extent_i[extent_i.nonzero()]
    extent_mean = np.round(100 * np.mean(extent), 2)

    # objectids = [int(oid) for oid in list(data.OBJECTID.unique())]
    #
    # oid_groups = data.groupby(by='OBJECTID')
    wb_extent = {}
    for oid, data in data.groupby(by='OBJECTID'):
        detects = data[detect_columns].sum(axis=1)
        oid_cells = data[all_columns].sum(axis=1)
        oid_extent_i = (detects/oid_cells).to_numpy()
        oid_extent = np.round(100 * np.mean(oid_extent_i[oid_extent_i.nonzero()]), 2)
        oid_extent = oid_extent if not np.isnan(oid_extent) else 0.0
        wb_extent[int(oid)] = oid_extent
    # for oid in objectids:
    #     oid_df = oid_groups.get_group(str(oid))
    #     detects = oid_df[detect_columns].sum(axis=1)
    #     all_cells = oid_df[all_columns].sum(axis=1)
    #     wb_extent[oid] = round(100 * (detects / all_cells).mean(), 2)
    return extent_mean, wb_extent


def calculate_chla(ci):
    return 6620.0 * ci


def calculate_magnitude(data: pd.DataFrame, detect_columns: list, all_columns: list):
    # bloom magnitude = 1/M * SUM(m=1->M) * 1/T * SUM(t=1->T) * SUM(p=1->P) * CI_cyano
    # p: represent the number of valid pixels in a lake or waterbody
    # t: the number of composite time sequence in a season or annual study period
    # M: the number of months in a season or annual
    # Bloom Magnitude

    # Equation 2 from magnitude publication
    # mean bloom magnitude = a_p/A_lake * 1/M * SUM(m=1->M) * 1/T * SUM(t=1->T) * SUM(p=1->P) CI(p,t,m)
    # mean bloom magnitude = (area_pixel / Area of lake)*(1/Number of Months)* For each month(m) in the timespan *
    # (1/Number of timesteps in month) * For each timestep(t) in month * For each pixel(p) in the waterbody : CI(p,t,m)

    # One date/step of the histogram represents the count of each pixel DN within the waterbody -> equivalent to Sum
    # of (DN_count) * (DN_value)

    # SUM(t=1->T) is the sum of all steps in the datespan.
    # SUM(m=1->M) is not utilized in this instance as the 'period' is always singular (only calculating for a single
    # timespan).

    CI_data = data[detect_columns]
    CI_values = [convert_dn(i) for i in range(1, 254)]
    T = int(data.shape[0]/data['OBJECTID'].nunique())
    magnitude = CI_data * CI_values
    magnitude['OBJECTID'] = data['OBJECTID']
    magnitude_wb = dict(magnitude.groupby(by='OBJECTID').sum().sum(axis=1)/T)
    wb_magnitude_update = {}
    for k, v in magnitude_wb.items():
        wb_magnitude_update[int(k)] = round(v, 2)

    waterbody_fids = get_waterbody_fids(return_dict=True)

    # Area-normalized magnitude = bloom magnitude / lake surface area (km2)
    area_normalized_magnitude = {}
    objectids = [int(oid) for oid in list(data.OBJECTID.unique())]

    for comid in objectids:
        wb_data = get_waterbody_by_fids(waterbody_fids[comid])
        wb_area = wb_data[0][0]['properties']['AREASQKM']
        pixel_area = 0.9            # 900sqmeters -> 0.9sqkm
        area_normalized_magnitude[int(comid)] = round((pixel_area/wb_area) * magnitude_wb[str(comid)], 2)

    chia_area_normalized_bloom = {}
    for k, v in area_normalized_magnitude.items():
        chia_area_normalized_bloom[int(k)] = round(calculate_chla(v), 2)
    return wb_magnitude_update, area_normalized_magnitude, chia_area_normalized_bloom