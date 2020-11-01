from context import property_analysis

import json
import pandas as pd
from datetime import datetime

with open('data/tfl_keys.json', 'r') as JSON:
    tflKeys = json.load(JSON)
jp = property_analysis.JourneyPlanner(app_id=tflKeys['app_id'], app_key=tflKeys['app_key'], rateLimit=0.14)
jp.load_json('tfl_results.json')

oldDf = pd.read_csv('data/journey_times_tcr.csv', index_col='postcode')
newDf = jp.get_df(resultsType='postcodes')

oldDf['dateTime'] = datetime(2020, 3, 2, 8).isoformat()
oldDf['to'] = '1000235'

# print(oldDf.dtypes)
# print(newDf.dtypes)
# print(oldDf)
# print(newDf)

combinedDf = oldDf.append(newDf)
combinedDf.drop(columns=['lat', 'long'], inplace=True)
print(combinedDf)
combinedDf.to_csv('journey_times_tcr.csv')