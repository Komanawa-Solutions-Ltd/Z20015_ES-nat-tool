"""

"""
import io
import os
import pandas as pd
import numpy as np
from datetime import datetime
import yaml
import tethys_utils as tu
import logging
from time import sleep
from pyproj import Proj, CRS, Transformer

pd.options.display.max_columns = 10

#############################################
### Parameters


base_path = os.path.realpath(os.path.dirname(__file__))

permit_csv = os.path.join(base_path, 'es_water_permits_current.csv')
sd_csv = os.path.join(base_path, 'es_stream_depletion_details.csv')

with open(os.path.join(base_path, 'parameters.yml')) as param:
    param = yaml.safe_load(param)

conn_config = param['remote']['connection_config']
bucket = param['remote']['bucket']

base_key = 'rma_permits/{name}.csv'

run_date = pd.Timestamp.today(tz='utc').round('s')
# run_date_local = run_date.tz_convert(ts_local_tz).tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
run_date_key = run_date.strftime('%Y%m%dT%H%M%SZ')


def read_s3_csv(s3, bucket, key):
    """

    """
    resp = s3.get_object(Bucket=bucket, Key=key)
    body1 = resp['Body'].read().decode()
    s_io = io.StringIO(body1)

    csv1 = pd.read_csv(s_io)

    return csv1


use_type_mapping = {'Dairying - Cows': 'irrigation', 'Water Supply - Rural': 'water_supply', 'Pasture Irrigation': 'irrigation', 'Crop Irrigation': 'irrigation', 'Stock Yard': 'stockwater', 'Water Supply - Town': 'water_supply', 'Quarrying': 'other', 'Recreational': 'other', 'Gravel extraction': 'other', 'Hydro-electric power generation': 'hydro_electric', 'Food Processing': 'other', 'Meat works': 'other', 'Tourism': 'other', 'Mining works': 'other', 'Industrial': 'other', 'Domestic': 'water_supply', 'Timber Processing incl Sawmills': 'other', 'Peat Harvesting/Processing': 'other', 'Milk and dairy industries': 'other', 'Gravel Wash': 'other'}


###########################################
#### Process csv files

### Permit file
permit1 = pd.read_csv(permit_csv)

permit2 = permit1[['AuthIRISID', 'CurrentStatus', 'CommencementDate', 'ExpiryDate', 'SubType', 'PrimaryIndustry', 'AuthExercised', 'AbstractionSiteName', 'IRISNorthing', 'IRISEasting', 'MaxAuthVol_LperSec', 'MaxVolm3perday', 'MaxVolm3peryear']].copy()

permit2.rename(columns={'AuthIRISID': 'permit_id', 'CurrentStatus': 'permit_status', 'CommencementDate': 'start_date', 'ExpiryDate': 'end_date', 'SubType': 'activity', 'PrimaryIndustry': 'use_type', 'AuthExercised': 'exercised', 'MaxAuthVol_LperSec': 'max_rate', 'MaxVolm3perday': 'max_daily_volume', 'MaxVolm3peryear': 'max_annual_volume'}, inplace=True)

permit2['use_type'] = permit2['use_type'].replace(use_type_mapping)

permit2['permit_id'] = permit2['permit_id'].apply(lambda x: x.split('AUTH-')[1]).str.strip()

permit2['start_date'] = pd.to_datetime(permit2['start_date'])
permit2['end_date'] = pd.to_datetime(permit2['end_date'])

activity1 = permit2.activity.str.split('(')
consump1 = activity1.apply(lambda x: x[1].split(')')[0].strip())
consump2 = consump1 == 'Consumptive'

hydro_group = activity1.apply(lambda x: x[0].strip().split(' Take')[0])

permit2['activity'] = 'take'
permit2['hydro_group'] = hydro_group
permit2['consumptive'] = consump2

permit2['exercised'] = permit2['exercised'] == 'Yes'

limit_cols = ['max_rate', 'max_daily_volume', 'max_annual_volume']

for c in limit_cols:
    permit2[c] = pd.to_numeric(permit2[c], errors='coerce')

permit3 = permit2.dropna(subset=limit_cols, how='all').dropna(subset=['AbstractionSiteName'])

permit4 = permit3.drop(['AbstractionSiteName', 'IRISNorthing', 'IRISEasting'], axis=1)

# permit4 = permit3.dropna(subset=['station_name'])


### Stations

def split_sites(permit_id, AbstractionSiteName, IRISNorthing, IRISEasting):
    """

    """
    sites = str(AbstractionSiteName).split('_')
    lats = [int(float(lat)) for lat in IRISNorthing.split(' ,')]
    lons = [int(float(lon)) for lon in IRISEasting.split(' ,')]
    permit = [permit_id] * len(sites)

    sites1 = list(zip(permit, sites, lons, lats))

    return sites1


sites_list = []

for i, row in permit3.iterrows():
    sites_list.extend(split_sites(row.permit_id, row.AbstractionSiteName, row.IRISNorthing, row.IRISEasting))

sites_df = pd.DataFrame(sites_list, columns=['permit_id', 'station', 'NZTMY', 'NZTMX'])
sites_df1 = sites_df[sites_df.station.str.contains('[A-Z]+\d*/\d+')].copy()

## Convert to lat and lon
from_crs = 2193
to_crs = 4326

from_crs1 = Proj(CRS.from_user_input(from_crs))
to_crs1 = Proj(CRS.from_user_input(to_crs))
trans1 = Transformer.from_proj(from_crs1, to_crs1)
points = np.array(trans1.transform(*sites_df1[['NZTMX', 'NZTMY']].values.T))
sites_df1['lon'] = points[1].round(5)
sites_df1['lat'] = points[0].round(5)

## Add altitude

k_key = param['source']['koordinates_key']

sites_u = sites_df1[['station', 'lon', 'lat']].drop_duplicates(subset=['station'])

alt1 = sites_u.apply(lambda x: tu.altitude_io.koordinates_raster_query('https://data.linz.govt.nz', k_key, '51768', x.lon, x.lat), axis=1)
alt2 = []
for a in alt1:
    try:
        alt2.extend([round(a[0]['value'], 3)])
    except:
        print('No altitude found, using -9999')
        alt2.extend([-9999])

sites_u['altitude'] = alt2

sites_df2 = pd.merge(sites_df1.drop(['NZTMX', 'NZTMY', 'lat', 'lon'], axis=1), sites_u, on='station')

### SD file

sd1 = pd.read_csv(sd_csv)

sd_cols = {'Consent Number': 'permit_id', 'Well Number': 'station', 'Depth': 'station_depth', 'Bore Specific Rate as Proportion of Whole Take (L/s)': 'station_max_rate', 'q/Q Total\nNo Flow Restriction': 'sd_ratio'}

sd2 = sd1[list(sd_cols.keys())].dropna(subset=['Consent Number']).rename(columns=sd_cols).copy()

sd2['permit_id'] = sd2['permit_id'].str.strip()

numeric_cols = ['station_depth', 'station_max_rate', 'sd_ratio']

for c in numeric_cols:
    sd2[c] = pd.to_numeric(sd2[c], errors='coerce')

## Compare the permit list between the files
permit_ids1 = sd2.permit_id.unique()

permit_ids2 = permit4.permit_id.unique()

missing_permits = permit_ids1[~np.in1d(permit_ids1, permit_ids2)]
print(missing_permits)

### Filtering
permit5 = permit4[permit4.permit_id.isin(sites_df2.permit_id.unique())].copy()

sd3 = sd2[sd2.permit_id.isin(permit5.permit_id.unique())].copy()

### Saving
s3 = tu.s3_connection(conn_config)

permit_key = base_key.format(name='es-permits')

obj1 = permit5.to_csv(index=False)

s3.put_object(Body=obj1, Bucket=bucket, Key=permit_key, ContentType='application/csv', Metadata={'run_date': run_date_key})

sd_key = base_key.format(name='es-sd-ratios')

obj1 = sd3.to_csv(index=False)

s3.put_object(Body=obj1, Bucket=bucket, Key=sd_key, ContentType='application/csv', Metadata={'run_date': run_date_key})

site_key = base_key.format(name='es-permits-stations')

obj1 = sites_df2.to_csv(index=False)

s3.put_object(Body=obj1, Bucket=bucket, Key=site_key, ContentType='application/csv', Metadata={'run_date': run_date_key})
