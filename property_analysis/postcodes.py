from math import sqrt
import time
import pandas as pd

class Postcodes(object):
    def __init__(self):
        self.postcodeDict = {}
        self.latlongDict = {}
        self.outcodes = []

    def load(self, csvPath: str, drop_exp=True, outcodes_to_drop=['CR90', 'N1P', 'N81', 'NW1W', 'NW26', 'SE1P'], dicts=True):
        '''
        Loads local csv of postcodes to a pandas DataFrame and (by default) cleans and extracts the important bits to object attributes.

        Latest csv postcode dataset for London can be found at: https://data.london.gov.uk/dataset/postcode-directory-for-london
        '''
        self.df = pd.read_csv(csvPath, low_memory=False)
        print('Postcodes: loaded csv')

        if drop_exp:
            self._drop_expired()

        if outcodes_to_drop:
            self._drop_outcodes(outcodes_to_drop)  # The default dropped outcodes are only for London-based post office sorting locations.

        if dicts:
            self._create_dicts()

    def _drop_expired(self):
        '''
        Drops all postcodes marked with an expiry date from the DataFrame.
        '''
        self.df = self.df[self.df['doterm'].isnull()]
        print('Postcodes: dropped expired')
    
    def _drop_outcodes(self, to_drop: list):
        '''
        Drops postcodes that start with the outcodes specified in 'to_drop' from the DataFrame.
        '''
        self.df['outcode'] = [postcode.split(' ')[0] for postcode in self.df['pcds']]
        self.df = self.df[~self.df['outcode'].isin(to_drop)]
        print('Postcodes: dropped selected outcodes')

    def _create_dicts(self):
        '''
        Adds two dictionaries to object attributes: 
        1) postcode -> (lat, long)
        2) (lat, long) -> postcode
        '''
        self.postcodeDict = {postcode: (lat, lon) for postcode, lat, lon in zip(self.df['pcds'], self.df['lat'], self.df['long'])}
        for postcode, latlong in self.postcodeDict.items():
            if latlong in self.latlongDict:
                self.latlongDict[latlong].append(postcode)
            else:
                self.latlongDict[latlong] = [postcode]
        print('Postcodes: created dicts')

    def get_outcodes(self):
        '''
        Returns sorted list of all unique outcodes (first part of a UK postcode) in these postcodes.
        '''
        if not self.outcodes:
            for postcode in self.postcodeDict.keys():
                if postcode.split(' ')[0] in self.outcodes:
                    continue
                else:
                    self.outcodes.append(postcode.split(' ')[0])
            self.outcodes.sort()

        return self.outcodes

    def df_add_ward_lad(self, csvPath):
        lookupDf = pd.read_csv(csvPath, index_col='WD19CD')
        lookupDf.drop(columns=['FID', 'LAD19CD'], inplace=True)
        lookupDf.rename(columns={'WD19NM': 'ward', 'LAD19NM': 'localAuthority'}, inplace=True)
        self.df.join(lookupDf, on='osward')
        return print('Postcodes: Added Ward & Local Authority District names to df.')

    def df_add_oac(self, csvPath):
        oacDf = pd.read_csv(csvPath)
        oacDf.index = [text.split(':')[0] for text in oacDf['Subgroup']]
        oacDf.drop(columns=['ObjectId'], inplace=True)
        oacDf.rename(columns={'Supergroup': 'oacSupergroup', 'Group_': 'oacGroup', 'Subgroup': 'oacSubgroup'}, inplace=True)
        self.df.join(oacDf, on='oac11')
        return print('Postcodes: Added Output Area Classifications to df.')

    def df_add_nearest_station(self, csvPath):

        def nearest_station(lat, lng):
            nearest = (None, None, float('inf'))
            for lat2,lng2 in latlongDict.keys():
                distance = sqrt((lat-lat2)**2 + (lng-lng2)**2)
                if distance < nearest[2]:
                    nearest = (*latlongDict[lat2, lng2], distance)
            return nearest

        print('Postcodes: Calculating nearest stations (may take a while)...')
        start = time.monotonic()
        stationsDf = pd.read_csv(csvPath)
        latlongDict = {(row.Latitude, row.Longitude): (row.Station, row.Zone) for row in stationsDf.itertuples()}
        self.df[['nearestStation', 'stationZone', 'stationDistance']] = [nearest_station(lat, lng) for lat, lng in zip(self.df['lat'], self.df['long'])]
        return print('Postcodes: Added nearest stations to df ({:.2f} secs).'.format(time.monotonic()-start))

    def get_df(self, drop_rename=True, dropExtended=False):
        df = self.df.copy()
        if drop_rename:
            df.drop(columns=['pcd', 'pcd2', 'doterm', 'oscty', 'ced', 'oslaua', 'osward', 'parish', 'usertype', 'oseast1m', 'osnrth1m', 'osgrdind', 'oshlthau', 'nhser', 'ctry', 'rgn', 'streg', 'eer', 'teclec', 'ttwa', 'pct', 'nuts', 'statsward', 'oa01', 'casward', 'park', 'lsoa01', 'msoa01', 'ur01ind', 'oac01', 'wz11', 'ccg', 'ru11ind', 'lep1', 'lep2', 'pfa', 'calncv', 'stp'], inplace=True)
            if dropExtended:
                df.drop(columns=['dointr', 'pcon', 'oa11', 'lsoa11', 'msoa11', 'bua11', 'buasd11', 'oac11'], inplace=True)
            df.rename(columns={'pcds': 'postcode', 'imd': 'deprivationRank', 'lat': 'latitude', 'long': 'longitude'}, inplace=True)
            df.set_index('postcode', inplace=True)
        return df

if __name__ == "__main__":

    p = Postcodes()
    p.load('postcodes_example.csv')
    # print(p.df[['pcds', 'lat', 'long']])
    print(len(p.postcodeDict))

    countDict = {latlong: len(postcodes) for latlong, postcodes in p.latlongDict.items()}
    sortedCounts = sorted(countDict.values())[::-1]
    print(sortedCounts[:100])

    # countDict2 = {count: p.latlongDict[latlong] for latlong, count in countDict.items()}
    # for count in sortedCounts[:10]:
    #     print(count, countDict2[count])