import ee
import geemap
import pandas as pd
import geopandas as gpd
import os
import multiprocessing
import time

# ee.Authenticate()
ee.Initialize()

Dir_save = 'C:\\Users\\Joao\\Pictures\\Time_Series_from_Polygons\\'
f_geom = "C:\\Users\\Joao\\Pictures\\Time_Series_from_Polygons\\Poligons.shp"

model = 'ECMWF/ERA5/DAILY'
variable = 'total_precipitation'
variable_name = ['Prec (mm)']
operation = 'mean'
data_format = '%Y%m%d'
save_name = 'Precipitacao_ECMWF'

N_processos = 10

begin = f"2000-01-01"
end = f"2000-01-31"

# kgm2s to mm
fator_conversao = 1000
Escala = 'D'

dt_geom = gpd.read_file(f_geom)

cods = dt_geom['gauge_id'].tolist()

def download(cod):
    print(cod)
    dt_geom_pivot = dt_geom[dt_geom['gauge_id'] == int(cod)]

    shape_geom = geemap.geopandas_to_ee(dt_geom_pivot)
    collection = ee.ImageCollection(model).filterDate(begin, end).select(variable)

    def iterr_collection(image):
        dict = image.reduceRegion(operation, shape_geom)
        return image.set(dict)

    collection_result = collection.map(iterr_collection)

    values = collection_result.aggregate_array(variable).getInfo()
    datas = collection_result.aggregate_array('system:index').getInfo()

    dt = pd.DataFrame(values, index=datas, columns=variable_name)
    dt.index = pd.to_datetime(dt.index, format=data_format)

    dt = dt * fator_conversao
    dt = dt.resample(Escala).mean()
    dt.to_csv(f'{Dir_save}\\{save_name}_{cod}.txt', sep='\t')

if __name__ == '__main__':
    # Determina o número de processos
    start_time = time.time()

    pool = multiprocessing.Pool(processes=N_processos)
    pool.map(download, cods)

    print("--- %s seconds ---" % (time.time() - start_time))