import asyncio
import json
from datetime import datetime
import pandas as pd

from .async_requests import AsyncRequests


class JourneyPlanner(object):
    def __init__(self, app_id, app_key, rateLimit=0.13):
        self.url = 'https://api.tfl.gov.uk/'

        self.app_id = app_id
        self.app_key = app_key
        self.rateLimit = rateLimit  # max: 500 per minute (interval: 0.12 secs)

    def load_postcodes(self, postcodeDict):
        self.postcodeDict = postcodeDict

    def request_journeys(self, endLocation, year, month, day, hour, limit=None):
        departDatetime = datetime(year=year, month=month, day=day, hour=hour, minute=00)
        if datetime.now() > departDatetime:
            raise Exception('Requested "departDatetime" is in the past')
        date = departDatetime.strftime('%Y%m%d')
        time = departDatetime.strftime('%H%M')

        params = {
            'date': date,
            'time': time,
            'walkingSpeed': 'Fast',
            'cyclePreference': 'None',
            'alternativeCycle': 'false',
            'alternativeWalking': 'true',
        }
        params.update({'app_id': self.app_id, 'app_key': self.app_key})

        self.results = {}

        async def run(endLocation, params):
            self.requests = AsyncRequests(rateLimit=self.rateLimit)
            await asyncio.gather(*[asyncio.ensure_future(self._fetch_journey(postcode, endLocation, params)) for postcode in list(self.postcodeDict.keys())[:limit]])
            await self.requests.close()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(run(endLocation, params))

        return print(f'JourneyPlanner: Collected {len(self.results)} results.')

    async def _fetch_journey(self, startPostcode, endLocation, params):
        url = f"{self.url}Journey/JourneyResults/{startPostcode}/to/{endLocation}"
        try:
            result, status = await self.requests.fetch(url, params)
            if status == 200:
                if 'journeys' in result:
                    self.results[startPostcode] = result
                    return print('Journey Planner: Fetched', startPostcode)
                else:
                    print(f'Error: _fetch_journey - {startPostcode} (Journey Planner failure)')
            elif status == 300:
                startLatLong = self.postcodeDict[startPostcode]
                url = "{}Journey/JourneyResults/{},{}/to/{}".format(
                    self.url, *startLatLong, endLocation)
                result, status = await self.requests.fetch(url, params)
                if 'journeys' in result: 
                    self.results[startPostcode] = result
                    return print('Journey Planner: Fetched', startPostcode, startLatLong)
                else:
                    print(f'Warning: _fetch_journey - {startPostcode} (Journey Planner failure)')
            else:
                print(f'Error: _fetch_journey - {startPostcode} (status code: {status})')

        except Exception as e:
            print(
                f"Error: _fetch_journey {startPostcode} {type(e).__name__} {e.args}")

    def to_json(self, path, type: dict or list = dict):
        with open(path, 'w') as f:
            if type == dict:
                json.dump(self.results, f, sort_keys=True)
            elif type == list:
                json.dump(self.postcodesList, f, sort_keys=True)
            else:
                raise Exception('to_json only works with type dict or list')

    def load_json(self, path):
        with open(path, 'r') as JSON:
            self.results = json.load(JSON)

    def get_df(self, resultsType: 'postcodes' or 'journeys'):
        if resultsType == 'postcodes':
            return pd.DataFrame(self.postcodesList).set_index('postcode', drop=True)
        elif resultsType == 'journeys':
            return pd.DataFrame(self.journeysList).set_index(['postcode', 'journeyIdx'], drop=True)
        else:
            raise Exception('get_df "resultsType" incorrect')

    @property
    def postcodesList(self):
        return [
        {
        'postcode': postcode, 
        'from': result['journeyVector']['from'], 
        'to': result['journeyVector']['to'], 
        'dateTime': result['searchCriteria']['dateTime'],
        'journeyTime': min([journey['duration'] for journey in result['journeys']])
        } 
        for postcode, result in self.results.items()]

    @property
    def journeysList(self):
        return [
        {
        'postcode': postcode, 
        'from': result['journeyVector']['from'], 
        'to': result['journeyVector']['to'], 
        'dateTime': result['searchCriteria']['dateTime'], 
        'dateTimeType': result['searchCriteria']['dateTimeType'],
        'journeyIdx': result['journeys'].index(journey),
        'duration': journey['duration'], 
        'legs': [{'mode': leg['mode']['name'], 'duration': leg['duration']} for leg in journey['legs']]
        } 
        for postcode, result in self.results.items() for journey in result['journeys']]

def journey_times_updater(csvPath, postcodeDict, tflKeysDict, destination, year, month, day, hour):
    with open(csvPath, 'r') as f:
        rows = f.readlines()
    headers = rows.pop(0).rstrip().split(',')
    colIdx = headers.index('postcode')
    collectedPostcodes = {row.split(',')[colIdx] for row in rows}

    newPostcodeDict = {postcode: latlong for postcode, latlong in postcodeDict.items() if postcode not in collectedPostcodes}
    print('Journey Planner: New postcodes:', len(newPostcodeDict))

    jp = JourneyPlanner(app_id=tflKeysDict['app_id'], app_key=tflKeysDict['app_key'], rateLimit=0.14)
    jp.load_postcodes(newPostcodeDict)
    jp.request_journeys(endLocation=destination, year=year, month=month, day=day, hour=hour, limit=None)
    
    if not jp.results:
        return print('Journey Planner: No new (working) postcodes since last update.')

    oldDf = pd.read_csv(csvPath, index_col='postcode')
    # try:
    newDf = jp.get_df(resultsType='postcodes')
    # except KeyError:
    #     return print('No new (working) postcodes since last update.')
    combinedDf = oldDf.append(newDf)
    # print(combinedDf)
    combinedDf.to_csv(csvPath)
    return print('Journey Planner: Postcodes added:', len(newDf))

if __name__ == "__main__":
    import json
    with open('tfl_keys.json', 'r') as JSON:
        tflKeys = json.load(JSON)

    from postcodes import Postcodes
    p = Postcodes()
    p.load('postcodes_example.csv')

    jp = JourneyPlanner(
        app_id=tflKeys['app_id'], app_key=tflKeys['app_key'], rateLimit=0.2)

    # jp.load_postcodes(p.postcodeDict)
    # jp.request_journeys(endLocation='1000235', date='20201102', limit=100)
    # jp.to_json('tfl_results.json')

    jp.load_json('tfl_results.json')

    print(jp.get_df(resultsType='journeys'))

    # from pprint import pprint
    # pprint(jp.postcodesList[:5])
    # pprint(jp.journeysList[:5])
    # pprint(jp.results['SW10 0JG']['journeyVector'])
    # pprint(jp.results['SW10 0JG']['recommendedMaxAgeMinutes'])
    # pprint(jp.results['SW10 0JG']['searchCriteria'])
    # pprint(jp.results['SW10 0JG']['stopMessages'])
    # pprint(jp.results['SW10 0JG']['journeys'][0].keys())