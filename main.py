import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import argparse
import time
from flaskr.db import p_set_geometry_tiles, set_geometry_tiles, save_data, get_waterbody_data, set_tile_bounds, set_waterbody_details_table, export_waterbody_details_table
from flaskr.utils import p_update_geometry_bounds, update_waterbody_fids
from flaskr.aggregate import aggregate, retry_failed, p_aggregate, generate_conus_image
from flaskr.report import generate_state_reports, generate_alpinelake_report
import logging
import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cyan-waterbody")

parser = argparse.ArgumentParser(description="CyAN Waterbody data database management functions.")
parser.add_argument('--set_tiles', default=False, type=bool, help='Reset the geometry to tiles mapping.')
parser.add_argument('--set_tile_bounds', default=False, type=bool, help='Set the bounds for the raster tifs')
parser.add_argument('--get_data', default=False, type=bool, help='Get all data from the database for a specified OBJECTID')
parser.add_argument('--year', default=None, type=int, help="Year of data image to process.")
parser.add_argument('--day', default=None, type=int, help="Day of the year of the data image to process.")
parser.add_argument('--daily', default=True, type=bool, help="Process daily data image file.")
parser.add_argument('--weekly', default=False, type=bool, help="Process weekly data image file.")
parser.add_argument('--objectid', default=None, type=int, help="OBJECTID of a waterbody for a single waterbody aggregation")
parser.add_argument('--aggregate', default=False, type=bool, help='Save the aggregated data for the images in image_dir to the database.')
parser.add_argument('--retry', default=False, type=bool, help='Retry failed aggregation attempts')
parser.add_argument('--set_wb_bounds', default=False, type=bool, help='Reset the waterbody bounds in the database from clipped rasters.')
parser.add_argument('--generate-state-reports', action='store_true', help='Generate reports for all CONUS states')
parser.add_argument('--generate-alpine-lake-report', action='store_true', help='Generate a report for all alpine lakes in CONUS, elevation of >= 5000ft')
parser.add_argument('--add_waterbody_fids', action='store_true', help='Update Waterbody database to include the FID column')
parser.add_argument('--add_waterbody_elevation', action='store_true', help='Update Waterbody database to include waterbody elevation data from USGS')
parser.add_argument('--export_waterbody_elevation', action='store_true', help='Export the waterbody elevation data table to csv')
parser.add_argument('--file', type=str, help="File path for input or output depending on the primary argument.")
parser.add_argument('--generate_conus_image', action='store_true', help='Test generating cyan image for day/year for all CONUS masking out all non-wb pixels.')

PARALLEL = True


def async_aggregate(year: int, day: int, daily: bool):
    logger.info("~~ Executing async waterbody aggregation for year: {}, day: {}, {}".format(year, day, "daily" if daily else "weekly"))
    t0 = time.time()
    logger.info("Inside async_aggregate()")
    try:
        completed = False
        offset = None
        while not completed:
            if PARALLEL:
                logger.info("Initiating p_aggregate()")
                data, offset, completed = p_aggregate(year, day, daily, offset=offset)
            else:
                logger.info("Initiating aggregate()")
                data, offset, completed = aggregate(year, day, daily, offset=offset)
            logger.info("Saving data.")
            save_data(year, day, data=data, daily=daily)
        logger.info("Completed processing waterbody aggregation for year: {}, day: {}, {}".format(year, day, "daily" if daily else "weekly"))
    except Exception as e:
        logger.critical("ERROR processing data for waterbody aggregation. Message: {}".format(e))
    t1 = time.time()
    logger.info(f"Completed waterbody {'daily' if daily else 'weekly'} aggregation for year: {year}, day: {day}, runtime: {round(t1 - t0, 4)} sec")
    generate_conus_image(day=int(day), year=int(year), daily=daily)
    t2 = time.time()
    logger.info(f"Completed generating conus {'daily' if daily else 'weekly'} image for year: {year}, day: {day}, runtime: {round(t2 - t1, 4)} sec")


def async_retry():
    retry_failed()
    retry_failed(daily=False)
    logger.info("Completed retry failed aggregations.")


if __name__ == "__main__":
    t0 = time.time()
    args = parser.parse_args()
    daily = args.daily if not args.weekly else False
    if args.set_tiles:
        if args.year is None or args.day is None:
            print("Mapping geometries to tiles requires reference tif, determined by year and day parameters.")
            exit()
        if PARALLEL:
            p_set_geometry_tiles(args.year, args.day, args.objectid)           # cpus=14 => 1:17:19
        else:
            set_geometry_tiles(args.year, args.day)
        logger.info("Completed setting waterbody to tile mapping.")
    elif args.aggregate:
        logger.info("Aggregating waterbodies for year: {}, day: {}, {}".format(args.year, args.day, "daily" if daily else "weekly"))
        completed = False
        offset = None
        while not completed:
            if PARALLEL:
                data, offset, completed = p_aggregate(args.year, args.day, daily=daily, offset=offset)
            else:
                data, offset, completed = aggregate(args.year, args.day, daily=daily, offset=offset)
            if data is None:
                logging.info("No images found for year: {}, day: {}, {}".format(args.year, args.day, "daily" if daily else "weekly"))
                exit()
            save_data(args.year, args.day, data=data, daily=daily)
        logger.info("Completed waterbody aggregation")
    elif args.get_data:
        data = get_waterbody_data(objectid=args.objectid, daily=daily)
        logger.info("Data: {}".format(data))
    elif args.retry:
        retry_failed()
        retry_failed(daily=False)
        logger.info("Completed retry failed aggregations.")
    elif args.set_wb_bounds:
        if args.year is None or args.day is None:
            print("Setting waterbody bounds requires reference tif, determined by year and day parameters.")
            exit()
        p_update_geometry_bounds(day=args.day, year=args.year)
        logger.info("Completed updating waterbody geometry bounds")
    elif args.set_tile_bounds:
        if args.year is None or args.day is None:
            print("Setting tile bounds requires reference tif, determined by year and day parameters.")
            exit()
        set_tile_bounds(int(args.year), int(args.day))
    elif args.add_waterbody_fids:
        print("Updating Waterbody table column FID with feature IDs")
        update_waterbody_fids()
        exit()
    elif args.add_waterbody_elevation:
        print("Updating Waterbody details table with elevation data")
        input_file = None
        if args.file:
            if os.path.exists(args.file):
                input_file = args.file
        set_waterbody_details_table(input_file=input_file)
        exit()
    elif args.export_waterbody_elevation:
        print("Exporting Waterbody details table to csv")
        wb_details = export_waterbody_details_table()
        wb_details.to_csv("waterbody-details.csv", index=False)
        exit()
    elif args.generate_state_reports:
        if args.year is None or args.day is None:
            print("Generating state reports requires the year and day parameters.")
            exit()
        generate_state_reports(year=int(args.year), day=int(args.day), parallel=True)
    elif args.generate_alpine_lake_report:
        if args.year is None or args.day is None:
            print("Generating alpine lake report requires the year and day parameters.")
            exit()
        generate_alpinelake_report(year=int(args.year), day=int(args.day), parallel=True)
    elif args.generate_conus_image:
        daily = True
        year = None
        day = None
        current_date = datetime.datetime.now()
        if "year" in args:
            year = int(args.year)
        else:
            year = current_date.year
        if "day" in args:
            day = int(args.day)
        else:
            day = current_date.timetuple().tm_yday
        if "daily" in args:
            # NOTE: Using parser.parse_args returns bools in args instead of strings
            daily = bool(args.daily)
        print("Daily: {}".format(daily))
        generate_conus_image(day=day, year=year, daily=daily)
    else:
        print("")
        logger.info("Invalid input arguments\n")
        parser.print_help()
    t1 = time.time()
    print("")
    logger.info("Runtime: {} sec".format(round(t1 - t0, 4)))
    exit()