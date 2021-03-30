import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from pathlib import Path
from rasterio import mask
import rasterio
import geopandas as gpd
import os
import datetime


IMAGE_DIR = os.getenv('IMAGE_DIR', os.path.join(".", "images"))


def get_images(year: int, day: int, daily: bool=True):
    """
    Returns the list of images in the IMAGE_DIR for the specified year and day,
    defaults to daily otherwise will look for weekly images
    :param year: Year of the image to be processed
    :param day: Day of the year of the image to be processed
    :param daily: Defaults to True, will look for daily data with the corresponding year and day values.
    :return: A list of paths to .tif images in the IMAGE_DIR directory.
    """
    if daily:
        base_image_name = "L{}{}.L3m_DAY_CYAN_CI_cyano_CYAN_CONUS_300m".format(year, f'{day:03}')
    else:
        date0 = datetime.date(year, 1, 1) + datetime.timedelta(days=day-1)
        date1 = date0 + datetime.timedelta(days=6)
        base_image_name = "L{}{}{}{}.L3m_7D_CYAN_CI_cyano_CYAN_CONUS_300m".format(date0.year, f'{date0.timetuple().tm_yday:03}', date1.year, f'{date1.timetuple().tm_yday:03}')

    image_files = [str(os.path.join(IMAGE_DIR, f)) for f in os.listdir(IMAGE_DIR) if
                   (".tif" in f and base_image_name in f)]
    return image_files


def clip_raster(raster, boundary, boundary_layer=None, boundary_crs=None, verbose: bool = False):
    """Clip the raster to the given boundary.

    Parameters
    ----------
    raster : string, pathlib.Path or rasterio.io.DataSetReader
        Location of or already opened raster.
    boundary : string, pathlib.Path or geopandas.GeoDataFrame
        The polygon by which to clip the raster.
    boundary_layer : string, optional
        For multi-layer files (like GeoPackage), specify the layer to be used.
    boundary_crs : string, optional
        The boundary polygon crs for re-projection, if required.

    Returns
    -------
    tuple
        Three elements:
            clipped : numpy.ndarray
                Contents of clipped raster.
            affine : affine.Affine()
                Information for mapping pixel coordinates
                to a coordinate system.
            crs : dict
                Dict of the form {'init': 'epsg:4326'} defining the coordinate
                reference system of the raster.

    """

    if isinstance(raster, Path):
        raster = str(raster)
    if isinstance(raster, str):
        raster = rasterio.open(raster)
    if isinstance(boundary, dict):
        boundary = gpd.GeoDataFrame(boundary).set_geometry('geometry')

    if not (boundary_crs == raster.crs or boundary_crs == raster.crs.data):
        boundary = boundary.to_crs(crs=raster.crs)
    coords = [boundary.geometry]

    # mask/clip the raster using rasterio.mask
    try:
        clipped, affine = mask.mask(dataset=raster, shapes=boundary, crop=True)
    except Exception as e:
        if verbose:
            print("ERROR: {}".format(e))
        return None

    if len(clipped.shape) >= 3:
        clipped = clipped[0]

    return clipped, affine, raster.crs
