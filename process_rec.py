"""

"""
import numpy as np
import os
import pandas as pd
import zstandard as zst
import tethys_utils as tu
import orjson
import geopandas as gpd
from gistools import vector, rec
from tethysts import Tethys
from matplotlib.pyplot import show

pd.options.display.max_columns = 10

############################################
### Parameters

base_path = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_path, 'parameters-permits.yml')) as param:
    param = yaml.safe_load(param)


base_path = '/media/sdb1/Data/NIWA'

rivers_shp = 'REC25_rivers/River_Lines.shp'
geojson1 = 'REC25_rivers.geojson'
geoz1 = 'REC25_rivers.geojson.zst'
geopkg1 = 'REC25_rivers.gpkg'

# geojson2 = 'REC25_watersheds.geojson'
watershed_shp = 'REC25_watersheds/ca240805-e6be-414e-8a39-579459acac2a2020230-1-1l4v6za.5259g.shp'
geojson2 = 'REC25_watersheds_simplified2.geojson'
geoz2 = 'REC25_watersheds.geojson.zst'
geopkg2 = 'REC25_watersheds.gpkg'

rivers_fields = ['Headwater', 'StreamOrde', 'upElev', 'downElev', 'nzsegment', 'FROM_NODE', 'TO_NODE', 'geometry']
watershed_fields = ['nzsegment', 'geometry']

##########################################
### The work


# with open(os.path.join(base_path, geojson1)) as json_file:
#     data = orjson.loads(json_file.read())

# b_data = orjson.dumps(data)

# data1 = [{'Headwater': r['properties']['Headwater'], 'StreamOrde': r['properties']['StreamOrde'], 'upElev': round(r['properties']['upElev'], 3), 'downElev': round(r['properties']['downElev'], 3), 'nzsegment': r['properties']['nzsegment'], 'FROM_NODE': r['properties']['FROM_NODE'], 'TO_NODE': r['properties']['TO_NODE'], 'geometry': r['geometry']} for r in data['features']]

# [r['geometry'].update({'coordinates': np.array(r['geometry']['coordinates']).round(5).tolist()}) for r in data1]

gdf1 = gpd.read_file(os.path.join(base_path, rivers_shp))
gdf2 = gdf1[rivers_fields].copy()
geo1 = gdf2.simplify(10)
gdf2['geometry'] = geo1

gdf3 = gdf2.to_crs(4326)

gdf3.to_file(os.path.join(base_path, geopkg1), layer='rec25_rivers', driver="GPKG")

z_data = tu.write_json_zstd(data1)

# data1 = tu.read_json_zstd(z_data)

# with open(os.path.join(base_path, geoz1), "wb") as f:
#     f.write(z_data)
#
# with open(os.path.join(base_path, geoz1), "rb") as f:
#     data2 = tu.read_json_zstd(f.read())



# with open(os.path.join(base_path, geojson2)) as json_file:
#     data = orjson.loads(json_file.read())
#
# # b_data = orjson.dumps(data)
#
# data1 = [{'nzsegment': r['properties']['nzsegment'], 'geometry': r['geometry'].copy()} for r in data['features']]
#
# [r['geometry'].update({'coordinates': [np.array(s).round(5).tolist() for s in r['geometry']['coordinates']]}) for r in data1]
# [r['geometry'].update({'coordinates': np.array(r['geometry']['coordinates']).round(5).tolist()}) for r in data1 if isinstance(r['geometry']['coordinates'], list)]

# bad1 = [r for r in data1 if isinstance(r['geometry']['coordinates'], float)]

# z_data = tu.write_json_zstd(data1)
#
# # data1 = tu.read_json_zstd(z_data)
#
# with open(os.path.join(base_path, geoz2), "wb") as f:
#     f.write(z_data)
#
# with open(os.path.join(base_path, geoz2), "rb") as f:
#     data2 = tu.read_json_zstd(f.read())



# file2 = os.path.join(base_path, geojson2)
#
#
# gdf1 = gpd.read_file(file2)
#
# gdf2 = gdf1[['nzsegment', 'geometry']]
#
# geo1 = gdf2['geometry'].simplify(0.00001)
#
# gdf2['geometry'] = geo1
#
# gdf2.to_file(os.path.join(base_path, geopkg1), layer='rec25_watersheds', driver="GPKG")
#
# gdf3 = gpd.read_file(os.path.join(base_path, geopkg1), layer='rec25_watersheds', driver="GPKG")


gdf1 = gpd.read_file(os.path.join(base_path, watershed_shp))
gdf2 = gdf1[watershed_fields].copy()
geo1 = gdf2.simplify(10)
gdf2['geometry'] = geo1

gdf3 = gdf2.to_crs(4326)

gdf3.to_file(os.path.join(base_path, geopkg2), layer='rec25_watersheds', driver="GPKG")














