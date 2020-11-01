from context import property_analysis

import json
import pandas as pd

with open('data/journey_times_tcr.csv', 'r') as f:
    rows = f.readlines()
headers = rows.pop(0).rstrip().split(',')
colIdx = headers.index('postcode')
collectedPostcodes = {row.split(',')[colIdx] for row in rows}

p = property_analysis.Postcodes()
# postcodesCsv = 'data/postcodes_example.csv'
postcodesCsv = 'https://data.london.gov.uk/download/postcode-directory-for-london/62b22f3f-25c5-4dd0-a9eb-06e2d8681ef1/london_postcodes-ons-postcodes-directory-MAY20.csv'
print('Loading postcodes csv...')
p.load(postcodesCsv)

newPostcodeDict = {postcode: latlong for postcode, latlong in p.postcodeDict.items() if postcode not in collectedPostcodes}
print('New postcodes:', len(newPostcodeDict))

with open('data/tfl_keys.json', 'r') as JSON:
    tflKeys = json.load(JSON)
jp = property_analysis.JourneyPlanner(app_id=tflKeys['app_id'], app_key=tflKeys['app_key'], rateLimit=0.14)
jp.load_postcodes(newPostcodeDict)

# jp.request_journeys(endLocation='1000013', year=2020, month=11, day=2, hour=8, limit=None)  # Destination: Bank Underground Station
jp.request_journeys(endLocation='1000235', year=2020, month=11, day=2, hour=8, limit=None)  # Destination: Tottenham Court Road Underground Station
jp.to_json('tfl_results.json')

print(jp.get_df(resultsType='postcodes'))

oldDf = pd.read_csv('data/journey_times_tcr.csv', index_col='postcode')
newDf = jp.get_df(resultsType='postcodes')
combinedDf = oldDf.append(newDf)
print(combinedDf)
combinedDf.to_csv('data/journey_times_tcr.csv')