from context import property_analysis

p = property_analysis.Postcodes()
postcodesCsv = 'data/postcodes_example.csv'
# postcodesCsv = 'https://data.london.gov.uk/download/postcode-directory-for-london/62b22f3f-25c5-4dd0-a9eb-06e2d8681ef1/london_postcodes-ons-postcodes-directory-MAY20.csv'
print('Loading postcodes csv...')
p.load(postcodesCsv)

rm = property_analysis.Rightmove(p.get_outcodes(), rateLimit=0.13)

saleParams = {
        'minBedrooms': '1',
        'maxBedrooms': '1',
        'radius': '0',
        'sortType': '1',
        'propertyTypes': 'bungalow,detached,flat,semi-detached,terraced',
        #'mustHave': '',
        'dontShow': 'newHome,retirement,sharedOwnership,auction',
        #'furnishTypes': '',
        #'keywords': '',
        }
rentParams = {
        'minBedrooms': '1',
        'maxBedrooms': '1',
        'radius': '0',
        'sortType': '1',
        'propertyTypes': 'flat',
        #'mustHave': '',
        'dontShow': 'houseShare,retirement',
        #'furnishTypes': '',
        #'keywords': '',
        }

# Search for sale:
# rm.search_properties(propType='sale', params=saleParams)#, limit=100)
# Search to rent:
rm.search_properties(propType='rent', params=rentParams)

rm.estimate_postcodes(p.latlongDict)
rm.to_json('rm_results.json')

rm.load_json('rm_results.json')

rmDf = rm.get_df(clean=True)
# filter any properties with latlong outside general London area
londonDf = rmDf[(rmDf.latitude > 51) & (rmDf.latitude < 52) & (rmDf.longitude > -0.75) & (rmDf.longitude < 0.75)]
print(londonDf)