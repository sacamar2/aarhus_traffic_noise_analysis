'''
This script generates a single csv for each requested field of a given table id
It is separated in single fields to avoid timeout from the opendata service
'''

from pandas import DataFrame
import requests as req

# CONSTANT VARIABLES
ROOT_DATA_URL='https://admin.opendata.dk/api/3/action/datastore_search_sql?sql='
CSV_DIR_PATH='./traffic_data_raw'

#You can get the id from here: https://www.opendata.dk/city-of-aarhus/realtids-trafikdata
TABLE_ID='b3eeb0ff-c8a8-4824-99d6-e0a3747c8b0d'

# COMPOSE THE DATABASE URL
query_template=f'SELECT * from "{TABLE_ID}"'
url_template=f'{ROOT_DATA_URL}{query_template}'

# BUILDING INDIVIDUAL FIELDS LIST
# These are the fields which doesnt have value itself and only bring metadata value
def_f_common=['_id','_full_text','date','sensor','REPORT_ID','TIMESTAMP','status'] # This is list is a default, these fields might not be on your table
f_common=[] # This is the list for the common fields on each created csv
fields=list(req.get(url_template).json()['result']['records'][0].keys())

for f in def_f_common:
    if f in fields:
        f_common.append(f)
        fields.remove(f)

success_cases=0
for f in fields:
    try:
        aux_url=url_template.replace("*",f"{','.join(f_common+[f])}")
        print(f'Getting data from {aux_url}')
        DataFrame(req.get(aux_url).json()['result']['records']
                  ).to_csv(index=False,path_or_buf=f'{CSV_DIR_PATH}/{f}.csv')
        success_cases+=1
    except:
        print(f'It wasnt possible to get the data for {f}')
        continue

if success_cases==0:
    try:
        print('As the download per field didnt work, we download all the table')
        DataFrame(req.get(url_template).json()['result']['records']
                  ).to_csv(index=False,path_or_buf=f'{CSV_DIR_PATH}/complete_table.csv')
    except Exception as e:
        print(f'It wasnt possible to download all the table because of {e}')


