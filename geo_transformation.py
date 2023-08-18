import pandas as pd

# CONSTANT VARIABLES

SOURCE_CSV_DIR_PATH='./traffic_data_raw'
DERIVED_CSV_DIR_PATH='./traffic_data'


metadata=pd.read_csv('roads_metadata.csv')
data=pd.read_csv('traffic_real_time.csv')

# The above line was because I wanted to have only the latest data, it is not needed.
filter_date=[(i.split('-')[0]=='2023' and i.split('-')[1]=='05') for i in data['TIMESTAMP']]
current_data=data[filter_date]

current_data['approx_noise']=(current_data['avgSpeed']*current_data['vehicleCount'])/current_data['medianMeasuredTime']

nonnan_data=current_data.dropna()

# MERGE METADATA WITH DATA

located_data=nonnan_data.merge(metadata,left_on='REPORT_ID',right_on='REPORT_ID')




# TRANSFORM LAN/LON + NOISE_DATA INTO GEOJSON
raw_geojson_data=located_data[['TIMESTAMP',
                            'POINT_1_LAT','POINT_1_LNG',
                            'POINT_2_LAT','POINT_2_LNG','approx_noise']]

p1_data=raw_geojson_data.drop_duplicates(subset=['POINT_1_LAT','POINT_1_LNG'])[['POINT_1_LAT','POINT_1_LNG','approx_noise']]
p2_data=raw_geojson_data.drop_duplicates(subset=['POINT_2_LAT','POINT_2_LNG'])[['POINT_2_LAT','POINT_2_LNG','approx_noise']]

p1_data['latitude']=p1_data['POINT_1_LAT']
p1_data['longitude']=p1_data['POINT_1_LNG']
p1_data.drop(['POINT_1_LAT','POINT_1_LNG'],axis=1,inplace=True)
p1_data.to_csv('p1_data.csv',index=False)

p2_data['latitude']=float(p2_data['POINT_2_LAT'])
p2_data['longitude']=p2_data['POINT_2_LNG']
p2_data.drop(['POINT_2_LAT','POINT_2_LNG'],axis=1,inplace=True)
p2_data.to_csv('p2_data.csv',index=False)

