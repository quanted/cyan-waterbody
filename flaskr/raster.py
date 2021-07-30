import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from pathlib import Path
from rasterio import mask, warp, crs, MemoryFile
from rasterio.merge import merge
from rasterio.enums import Resampling
from rasterio.profiles import DefaultGTiffProfile
from pyproj import Proj
from pyproj import transform as pyt
import types
import copy
import rasterio
import geopandas as gpd
import os
import datetime


IMAGE_DIR = os.getenv('IMAGE_DIR', "D:\\data\cyan_rare\\mounts\\images")
DST_CRS = 'EPSG:4326'


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


def get_images_by_tile(tile: list, n_limit: int = 90):
    """
    Returns the list of images in the IMAGE_DIR for the specified tile going back n_limit days from current date.
    :param tile: Tiles of the images to collect, example [1_2, 1_3]
    :param n_limit: The number of days from the current date to get available images for.
    :return: A list of paths to .tif images in the IMAGE_DIR directory.
    """
    n_date = datetime.datetime.utcnow() + datetime.timedelta(days=(-1 * n_limit) - 1)
    image_files = []
    for f in os.listdir(IMAGE_DIR):
        if any(t in f for t in tile) and ".tif" in f and "DAY" in f:
            i_year = f[1:5]
            i_yday = f[5:9]
            date0 = datetime.date(int(i_year), 1, 1) + datetime.timedelta(days=int(i_yday)-1)
            if date0 >= n_date:
                image_files.append(str(os.path.join(IMAGE_DIR, f)))
    return image_files


def clip_raster(raster, boundary, boundary_layer=None, boundary_crs=None, verbose: bool = False, raster_crs: dict = None):
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
    crs_0 = None
    if isinstance(raster, Path):
        raster = str(raster)
    if isinstance(raster, str):
        raster = rasterio.open(raster)
    if isinstance(boundary, dict):
        boundary = gpd.GeoDataFrame(boundary).set_geometry('geometry')

    if isinstance(raster, types.GeneratorType):
        crs_0 = DST_CRS
        boundary = boundary.to_crs(crs=DST_CRS)
    elif not (boundary_crs == raster.crs or boundary_crs == raster.crs.data):
        crs_0 = raster.crs
        boundary = boundary.to_crs(crs=raster.crs)

    height, width = None, None
    bounds = None

    # mask/clip the raster using rasterio.mask
    clipped, affine = None, None
    try:
        if isinstance(raster, types.GeneratorType):
            for r in raster:
                bounds = r.bounds
                height = r.height
                width = r.width
                clipped, affine = mask.mask(dataset=r, shapes=boundary, crop=True, )
        else:
            bounds = raster.bounds
            height = raster.height
            width = raster.width
            clipped, affine = mask.mask(dataset=raster, shapes=boundary, crop=True, )
    except Exception as e:
        if verbose:
            print("ERROR: {}".format(e))
        return None

    if len(clipped.shape) >= 3:
        clipped = clipped[0]

    bbox = None
    if raster_crs:
        crs = rasterio.crs.CRS.from_dict(raster_crs)
        reproject_raster = copy.copy(clipped)
        transform, width, height = warp.calculate_default_transform(
            crs_0, crs, width, height, *bounds)
        reproject_raster, reproject_affine = warp.reproject(
            clipped,
            reproject_raster,
            src_transform=affine,
            src_crs=crs_0,
            dst_tranform=transform,
            dst_crs=crs,
            resampling=Resampling.nearest
        )
        clipped = reproject_raster
        affine = reproject_affine
        bounds = rasterio.transform.array_bounds(
            height=reproject_raster.shape[0],
            width=reproject_raster.shape[1],
            transform=reproject_affine
        )
        proj0 = Proj(crs)
        proj1 = Proj('epsg:4326')
        bbox = [pyt(proj0, proj1, bounds[2], bounds[1]), pyt(proj0, proj1, bounds[0], bounds[3])]

    return clipped, affine, raster_crs, bbox


def get_raster_bounds(image_path):
    dst_crs = 'EPSG:4326'
    raster = rasterio.open(image_path)
    bounds = warp.transform_bounds(src_crs=raster.crs, dst_crs=dst_crs, left=raster.bounds.left,
                                   bottom=raster.bounds.bottom, right=raster.bounds.right, top=raster.bounds.top)
    return bounds


def mosaic_rasters(images):
    src_crs = rasterio.open(images[0]).crs
    mosaic, out_trans = merge(images)
    mosaic, out_trans = warp.reproject(
        source=mosaic,
        src_crs=src_crs,
        src_transform=out_trans,
        dst_crs=DST_CRS,
        resampling=Resampling.nearest
    )
    mosaic_reader_gen = get_dataset_reader(mosaic, out_trans, crs=DST_CRS)
    return mosaic_reader_gen


def get_colormap(image):
    raster = rasterio.open(image)
    return raster.colormap(1)


def get_dataset_reader(data, transform, crs):
    profile = DefaultGTiffProfile(count=1)
    profile.update(transform=transform, height=data.shape[1], width=data.shape[2], crs=crs)
    dataset_reader = None
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(data)
            del data
        with memfile.open() as dataset:
            # dataset_reader = dataset
            yield dataset
    # return dataset_reader
