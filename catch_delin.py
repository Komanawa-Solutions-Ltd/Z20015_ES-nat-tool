"""

"""
import numpy as np
import os
import pandas as pd
import zstandard as zst
import tethys_utils as tu
import orjson
import geopandas as gpd
import yaml
from gistools import vector, rec
from tethysts import Tethys
from matplotlib.pyplot import show

pd.options.display.max_columns = 10

############################################
### Parameters

base_path = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_path, 'parameters.yml')) as param:
    param = yaml.safe_load(param)


file_path = '/media/sdb1/Data/NIWA'

rivers_shp = 'REC25_rivers/rec25_rivers.shp'
watershed_shp = 'REC25_watersheds/rec25_watersheds.shp'

remote = param['remote']
bucket = param['remote']['bucket']
conn_config = param['remote']['connection_config']

dataset_ids = ['238c2f396d316fe861a8e545', '4ae05d099af292fec48792ec']

catch_shp = 'es_flow_sites_catchments_delin_2020-11-20.shp'
catch_gpkg = 'es_flow_sites_catchment_delin_2020-11-20.gpkg'

pts_seg_shp = 'es_flow_sites_segments_2020-11-20.shp'

##########################################
### Get the data

t1 = Tethys([remote])

all_stns_list = []

for ds_id in dataset_ids:
    stns1 = t1.get_stations(ds_id)
    all_stns_list.extend(stns1)

all_stns = pd.DataFrame([{'dataset_id': s['dataset_id'], 'station_id': s['station_id'], 'ref': s['ref'], 'lon': s['geometry']['coordinates'][0], 'lat': s['geometry']['coordinates'][1]} for s in all_stns_list])

all_stns = all_stns.drop_duplicates('station_id')

stns_gdf1 = vector.xy_to_gpd('station_id', 'lon', 'lat', all_stns, 4326)

sites1 = pd.merge(stns_gdf1, all_stns.drop(['lon', 'lat'], axis=1), on='station_id')

rec_streams1 = gpd.read_file(os.path.join(file_path, rivers_shp))

rec_catch1 = gpd.read_file(os.path.join(file_path, watershed_shp))

########################################
### Catchment delineation

sites = sites1.to_crs(2193)
# rec_streams = rec_streams1.to_crs(2193)
# rec_catch = rec_catch1.to_crs(2193)

rec_streams = rec_streams1.copy()
rec_catch = rec_catch1.copy()

rec_shed1, reaches, pts_seg = catch_delineate(sites, rec_streams, rec_catch, segment_id_col='nzsegment', from_node_col='FROM_NODE', to_node_col='TO_NODE', max_distance=1000, returns='all')

rec_shed1.to_file(os.path.join(file_path, catch_shp))
rec_shed1.to_file(os.path.join(file_path, catch_gpkg), layer='es_flow_sites_catch_delin', driver="GPKG")

pts_seg.to_file(os.path.join(file_path, pts_seg_shp))


