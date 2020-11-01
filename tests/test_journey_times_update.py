from context import property_analysis

import json

p = property_analysis.Postcodes()
postcodesCsv = 'https://data.london.gov.uk/download/postcode-directory-for-london/62b22f3f-25c5-4dd0-a9eb-06e2d8681ef1/london_postcodes-ons-postcodes-directory-MAY20.csv'
print('Loading postcodes csv...')
p.load(postcodesCsv)

with open('data/tfl_keys.json', 'r') as JSON:
    tflKeys = json.load(JSON)

property_analysis.journey_times_updater(
    csvPath='data/journey_times_tcr.csv',
    postcodeDict=p.postcodeDict, 
    tflKeysDict=tflKeys,
    destination='1000235',  # Destination: Tottenham Court Road Underground Station
    year=2020, month=11, day=2, hour=8
    )