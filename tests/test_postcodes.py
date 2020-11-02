from context import property_analysis

p = property_analysis.Postcodes()
# postcodesCsv = 'data/postcodes_example.csv'
postcodesCsv = 'https://data.london.gov.uk/download/postcode-directory-for-london/62b22f3f-25c5-4dd0-a9eb-06e2d8681ef1/london_postcodes-ons-postcodes-directory-MAY20.csv'
print('Loading postcodes csv...')
p.load(postcodesCsv)

p.df_add_ward_lad('data/Ward_to_Local_Authority_District_(December_2019)_Lookup_in_the_United_Kingdom.csv')
p.df_add_oac('data/Output_Area_Classification__December_2011__in_the_United_Kingdom.csv')
p.df_add_nearest_station('data/London stations.csv')
print(p.get_df())