import ee
import geemap
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime as dt
from tqdm import tqdm


def img_datetime(img):
    imgdate = dt.fromtimestamp(img.get("system:time_start").getInfo() / 1000.)
    return imgdate


def img_ids(imgcollection):
    ids = [item.get('id') for item in imgcollection.getInfo().get('features')]
    return ids


def pixels_values(imgcollection, geometries, band_name, id_name):
    ids_list = img_ids(imgcollection)

    pixel_all_values = []
    for id in tqdm(ids_list):
        image = ee.Image(id).select(band_name)

        image_pixels = image.reduceRegions(
            reducer=ee.Reducer.mean(),
            collection=geometries,
            # scale=9999,
        )
        # .filter(ee.Filter.neq('mean', None))

        img_date = img_datetime(image)
        values = []
        for pixel in image_pixels.getInfo()['features']:
            geom = pixel['geometry']['coordinates']
            pixel_id = pixel['properties'][id_name]

            try:
                mean = pixel['properties']['mean']
            except:
                mean = -999

            values.append([mean, geom, pixel_id])
            # values.append([img_date, mean, geom])

        pixel_all_values.append([img_date, values])
        # pixel_all_values.append(values)

    return pixel_all_values

# For the first time running the code, uncoment the line below
# ee.Authenticate()
ee.Initialize()

f_geom = "C:\\Users\\Joao\\Pictures\\pontos_t_agua.shp"
f_save = "C:\\Users\\Joao\\Pictures\\pontos_temp_ar.txt"

dt_geom = gpd.read_file(f_geom)
dt_geom = dt_geom[["Codigo", "geometry"]]

points_shp = geemap.shp_to_ee((f_geom))

begin = f"2020-01-01"
end = f"2020-01-31"

dataset = ee.ImageCollection('ECMWF/ERA5/DAILY') \
    .select(['mean_2m_air_temperature']) \
    .filterDate(begin, end) \
    .filterBounds(points_shp) \
    .sort('system:time_start')

dataset = dataset.map(lambda img: img.clip(points_shp))

pixels_timeserie = pixels_values(dataset, points_shp, "mean_2m_air_temperature", id_name='Codigo')

dfs = [pd.DataFrame(pt[1], columns=[pt[0].strftime("%Y/%m/%d"), "geom", "id"]) for pt in pixels_timeserie]
df_concat = pd.concat(dfs, axis=1, join="inner")
df_concat = df_concat.drop(["geom", "id"], axis=1).T
df_concat.columns = dt_geom['Codigo'].tolist()

df_concat.to_csv(f_save, sep='\t')
