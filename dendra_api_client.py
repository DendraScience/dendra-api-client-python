'''
Dendra API Client

author: Collin Bode
email: collin@berkeley.edu

Python wrapper functions around REST API calls to the Dendra APIv2.
Simplifies pulling time-series data from https://dendra.science.
Dendra API pages records in sets of 2,016 maximum; this library handles that automatically.

Function groups:

Helper functions
    time_utc(str_time="")
    time_format(dt_time=None, time_type='local')
    authenticate(email)

List: returns a simple list of available objects
    get_organization_id(orgslug, needs_auth=False)
    list_organizations(orgslug='all', needs_auth=False)
    list_stations(orgslug='all', query_add=None, needs_auth=False)
    list_datastreams_by_station_id(station_id, query_add=None, needs_auth=False)
    list_datastreams_by_query(query_add=None, station_id='', needs_auth=False)
    list_datastreams_by_medium_variable(medium='', variable='', aggregate='', station_id='', orgslug='', query_add=None, needs_auth=False)
    list_datastreams_by_measurement(measurement='', aggregate='', station_id=None, orgslug='', query_add=None, needs_auth=False)

Get_Meta: returns full metadata objects
    get_meta_organization(orgslug='', orgid='', needs_auth=False)
    get_meta_station_by_id(station_id, query_add=None, needs_auth=False)
    get_meta_datastream_by_id(datastream_id, query_add=None, needs_auth=False)
    get_meta_annotation(annotation_id, query_add=None, needs_auth=False)
    get_datastream_id_from_dsid(dsid, orgslug='all', station_id='')

Get_Datapoints: returns timestamp/value pairs as a Pandas DataFrame
    get_datapoints(datastream_id, begins_at, ends_before=None, time_type='local', name='default', needs_auth=False)
    get_datapoints_from_id_list(datastream_id_list, begins_at, ends_before=None, time_type='local', needs_auth=False)
    get_datapoints_from_station_id(station_id, begins_at, ends_before=None, time_type='local', needs_auth=False)

API documentation: https://api-v2-docs.dendra.science/
Code repository: https://github.com/DendraScience/dendra-api-client-python
'''

import requests
import pandas as pd
import datetime as dt
import pytz
from dateutil import tz
from dateutil.parser import parse
from getpass import getpass
import concurrent.futures


url = 'https://api.edge.dendra.science/v2/'
headers = {"Content-Type": "application/json"}


###########################################################
# Internal Helpers

def _check_auth():
    """Raise RuntimeError if no auth token is present in headers."""
    if 'Authorization' not in headers:
        raise RuntimeError("Authentication required. Call authenticate() first.")


def _validate_mongo_id(value, name='ID'):
    """Raise TypeError/ValueError if value is not a valid 24-character MongoDB ObjectId string."""
    if not isinstance(value, str):
        raise TypeError(f"Invalid {name}: expected a string, got {type(value).__name__}")
    if len(value) != 24:
        raise ValueError(f"Invalid {name}: must be 24 characters, got {len(value)}")


###########################################################
# Time Helpers & Authentication

def time_utc(str_time=""):
    """Parse a time string and return a UTC datetime. Returns current UTC time if no string given."""
    if str_time == "":
        return dt.datetime.now(pytz.utc)
    dt_time = parse(str_time)
    if dt_time.tzinfo != pytz.utc:
        dt_time = dt_time.astimezone(pytz.utc)
    return dt_time


def time_format(dt_time=None, time_type='local'):
    """Format a datetime as an ISO 8601 string. Defaults to the current local time."""
    if dt_time is None:
        dt_time = dt.datetime.now()
    if time_type == 'utc':
        return dt.datetime.strftime(dt_time, "%Y-%m-%dT%H:%M:%SZ")
    return dt.datetime.strftime(dt_time, "%Y-%m-%dT%H:%M:%S")


def authenticate(email):
    """Authenticate with Dendra and store the access token. Required for non-public datasets."""
    data = {
        'email': email,
        'strategy': 'local',
        'password': getpass()
    }
    r = requests.post(url + 'authentication', json=data)
    assert r.status_code == 201
    headers['Authorization'] = r.json()['accessToken']


###########################################################
# List Functions

def get_organization_id(orgslug, needs_auth=False):
    """Return the MongoDB _id for an organization given its slug (e.g. 'erczo', 'ucnrs')."""
    if needs_auth:
        _check_auth()
    query = {'$select[_id]': 1, 'slug': orgslug}
    r = requests.get(url + 'organizations', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data'][0]['_id']


def list_organizations(orgslug='all', needs_auth=False):
    """Return a list of organizations. Pass an orgslug to filter to one organization."""
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$select[slug]': 1,
    }
    if orgslug != 'all':
        query['slug'] = orgslug
    r = requests.get(url + 'organizations', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


def list_stations(orgslug='all', query_add=None, needs_auth=False):
    """Return a list of stations. Optionally filter by orgslug or extend with query_add."""
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$select[slug]': 1,
        '$limit': 2016,
    }
    if orgslug != 'all':
        org_list = list_organizations(orgslug)
        if not org_list:
            return 'ERROR: no organizations found with that acronym.'
        query['organization_id'] = org_list[0]['_id']
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'stations', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


def list_datastreams_by_station_id(station_id, query_add=None, needs_auth=False):
    """Return a list of datastreams for a given station_id."""
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        'station_id': station_id,
        '$limit': 2016,
    }
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


def list_datastreams_by_query(query_add=None, station_id='', needs_auth=False):
    """Return a list of datastreams filtered by an arbitrary query dict and optional station_id."""
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$limit': 2016,
    }
    if query_add is not None:
        query.update(query_add)
    if station_id:
        query['station_id'] = station_id
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


def list_datastreams_by_medium_variable(medium='', variable='', aggregate='', station_id='', orgslug='', query_add=None, needs_auth=False):
    """Return datastreams matching medium/variable/aggregate class tags.
    medium: Air, Water, Soil, etc. variable: Temperature, Moisture, etc. aggregate: Minimum, Average, Maximum, Cumulative
    """
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$limit': 2016,
    }
    if medium:
        query['terms_info.class_tags[$all][0]'] = 'ds_Medium_' + medium
    if variable:
        query['terms_info.class_tags[$all][1]'] = 'ds_Variable_' + variable
    if aggregate:
        query['terms_info.class_tags[$all][2]'] = 'ds_Aggregate_' + aggregate
    if station_id:
        query['station_id'] = station_id
    if orgslug:
        query['organization_id'] = get_organization_id(orgslug)
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


def list_datastreams_by_measurement(measurement='', aggregate='', station_id=None, orgslug='', query_add=None, needs_auth=False):
    """Return datastreams matching a Dendra measurement vocabulary term.
    measurement: e.g. AirTemperature, VolumetricWaterContent, RainfallCumulative (no spaces, capitalized).
    See https://dendra.science/vocabulary for the full list.
    """
    if needs_auth:
        _check_auth()
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$limit': 2016,
    }
    if measurement:
        query['terms_info.class_tags[$all][0]'] = 'dq_Measurement_' + measurement
    if aggregate:
        query['terms_info.class_tags[$all][2]'] = 'ds_Aggregate_' + aggregate
    if station_id:
        query['station_id'] = station_id
    if orgslug:
        query['organization_id'] = get_organization_id(orgslug)
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data']


###########################################################
# Get Metadata Functions

def get_meta_organization(orgslug='', orgid='', needs_auth=False):
    """Return full metadata for an organization by slug or id."""
    if needs_auth:
        _check_auth()
    if orgslug and not orgid:
        orgid = get_organization_id(orgslug)
    if not orgid:
        raise ValueError("Provide either orgslug or orgid.")
    r = requests.get(url + 'organizations', headers=headers, params={'_id': orgid})
    assert r.status_code == 200
    return r.json()['data'][0]


def get_meta_station_by_id(station_id, query_add=None, needs_auth=False):
    """Return full metadata for a station given its MongoDB _id."""
    if needs_auth:
        _check_auth()
    _validate_mongo_id(station_id, 'station_id')
    query = {'_id': station_id}
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'stations', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data'][0]


def get_meta_datastream_by_id(datastream_id, query_add=None, needs_auth=False):
    """Return full metadata for a datastream given its MongoDB _id."""
    if needs_auth:
        _check_auth()
    _validate_mongo_id(datastream_id, 'datastream_id')
    query = {'_id': datastream_id}
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data'][0]


def get_meta_annotation(annotation_id, query_add=None, needs_auth=False):
    """Return full metadata for an annotation given its MongoDB _id."""
    if needs_auth:
        _check_auth()
    _validate_mongo_id(annotation_id, 'annotation_id')
    query = {'_id': annotation_id}
    if query_add is not None:
        query.update(query_add)
    r = requests.get(url + 'annotations', headers=headers, params=query)
    assert r.status_code == 200
    return r.json()['data'][0]


def get_datastream_by_id(datastream_id, query_add=None):
    """Deprecated. Use get_meta_datastream_by_id instead."""
    return get_meta_datastream_by_id(datastream_id, query_add)


def get_datastream_id_from_dsid(dsid, orgslug='all', station_id=''):
    """Translate a legacy SensorDB integer DSID to a Dendra MongoDB datastream_id."""
    query = {'$limit': 2016}
    if station_id:
        query['station_id'] = station_id

    org_list = list_organizations(orgslug)
    if not org_list:
        print('ERROR: no organizations found with that acronym.')
        return ''

    all_data = []
    for org in org_list:
        query_org = query.copy()
        query_org['organization_id'] = org['_id']
        r = requests.get(url + 'datastreams', headers=headers, params=query_org)
        assert r.status_code == 200
        all_data.extend(r.json()['data'])

    for ds in all_data:
        for ref in ds.get('external_refs', []):
            if ref['type'] == 'odm.datastreams.DatastreamID' and int(ref['identifier']) == dsid:
                return ds['_id']


###########################################################
# Get Datapoints Functions

def get_datapoints(datastream_id, begins_at, ends_before=None, time_type='local', name='default', needs_auth=False):
    """Return datapoints for one datastream as a Pandas DataFrame with a timestamp index.
    ends_before defaults to now. time_type is 'local' (default) or 'utc'.
    If time_type='utc', timestamps must end with 'Z'.
    """
    if needs_auth:
        _check_auth()
    _validate_mongo_id(datastream_id, 'datastream_id')

    if ends_before is None:
        ends_before = time_format()
    if time_type == 'utc' and not ends_before.endswith('Z'):
        ends_before += 'Z'

    query = {
        'datastream_id': datastream_id,
        'time[$gte]': begins_at,
        'time[$lt]': ends_before,
        '$sort[time]': '1',
        '$limit': '2016',
    }
    if time_type == 'utc':
        time_col = 't'
    else:
        query['time_local'] = 'true'
        time_col = 'lt'

    r = requests.get(url + 'datapoints', headers=headers, params=query)
    if r.status_code != 200:
        return r.status_code

    # Page through all results (Dendra max 2,016 records per request)
    all_data = []
    page = r.json()['data']
    while page:
        all_data.extend(page)
        query.pop('time[$gte]', None)
        query['time[$gt]'] = page[-1][time_col]
        r = requests.get(url + 'datapoints', headers=headers, params=query)
        assert r.status_code == 200
        page = r.json()['data']

    if all_data:
        df = pd.DataFrame.from_records(all_data)
    else:
        df = pd.DataFrame(columns=['lt', 't', 'v'])

    if name == 'default':
        ds_meta = get_meta_datastream_by_id(datastream_id, {'$select[name]': 1, '$select[station_id]': 1})
        stn_meta = get_meta_station_by_id(ds_meta['station_id'], {'$select[slug]': 1})
        stn = stn_meta['slug'].replace('-', ' ').title().replace(' ', '')
        datastream_name = stn + '_' + ds_meta['name'].replace(' ', '_')
    else:
        datastream_name = name

    df.rename(columns={'lt': 'timestamp_local', 't': 'timestamp_utc', 'v': datastream_name}, inplace=True)
    df['timestamp_local'] = pd.to_datetime(df['timestamp_local'], format='ISO8601')
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], format='ISO8601', utc=True)

    if time_type == 'utc':
        df.set_index('timestamp_utc', inplace=True, drop=True)
    else:
        df.set_index('timestamp_local', inplace=True, drop=True)

    return df


def get_datapoints_from_id_list(datastream_id_list, begins_at, ends_before=None, time_type='local', needs_auth=False):
    """Return a merged DataFrame of datapoints from multiple datastream IDs (threaded).
    The first datastream to complete sets the time index.
    """
    if needs_auth:
        _check_auth()
    if ends_before is None:
        ends_before = time_format()

    df = None
    future_to_dsid = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for dsid in datastream_id_list:
            future = executor.submit(get_datapoints, dsid, begins_at, ends_before, time_type, 'default')
            future_to_dsid[future] = dsid

        for i, future in enumerate(concurrent.futures.as_completed(future_to_dsid)):
            dsid = future_to_dsid[future]
            dftemp = future.result()

            if isinstance(dftemp, int):
                print(f"{i} ERROR: failed to retrieve datastream ({dsid}). Check authentication or ID.")
                continue
            if dftemp.empty:
                print(f"Datastream ({dsid}) has no data for this time period. Skipping.")
                continue

            if df is None:
                df = dftemp
                print(f"{i} {dftemp.columns[1]} NEW dataframe created!")
            else:
                if 'q' in dftemp.columns:
                    dftemp.drop('q', axis=1, inplace=True)
                if 'timestamp_utc' in dftemp.columns:
                    dftemp.drop('timestamp_utc', axis=1, inplace=True)
                df = df.merge(dftemp, how='left', left_index=True, right_index=True)
                print(f"{i} {dftemp.columns[0]} added.")

    return df


def get_datapoints_from_station_id(station_id, begins_at, ends_before=None, time_type='local', needs_auth=False):
    """Return a DataFrame with all datastreams for a station for the given time period."""
    if needs_auth:
        _check_auth()
    if ends_before is None:
        ends_before = time_format()
    ds_list = list_datastreams_by_station_id(station_id)
    dlist = [ds['_id'] for ds in ds_list]
    return get_datapoints_from_id_list(dlist, begins_at, ends_before, time_type)


###########################################################
# Deprecated Functions

def _lookup_datapoints_subquery(bigjson, query, endpoint='datapoints/lookup'):
    r = requests.get(url + endpoint, headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    if not bigjson:
        bigjson = rjson
    else:
        for i in range(len(bigjson)):
            bigjson[i]['datapoints']['data'].extend(rjson[i]['datapoints']['data'])
    return bigjson


def lookup_datapoints(query, endpoint='datapoints/lookup', interval=5):
    """Deprecated. Use get_datapoints instead."""
    begins_at_original = parse(query['time[$gte]'])
    if 'time[$lt]' in query:
        ends_before_original = parse(query['time[$lt]'])
    else:
        ends_before_original = dt.datetime.now(tz.tzlocal()).astimezone(pytz.utc)

    interval2k = dt.timedelta(minutes=interval) * 2016
    begins_at = begins_at_original
    ends_before = begins_at_original + interval2k
    bigjson = {}

    while ends_before < ends_before_original and begins_at < ends_before_original:
        bigjson = _lookup_datapoints_subquery(bigjson, query, endpoint)
        begins_at = ends_before
        ends_before = begins_at + interval2k
    bigjson = _lookup_datapoints_subquery(bigjson, query, endpoint)

    max_records = pd.date_range(
        start=begins_at_original, end=ends_before_original, tz='UTC', freq=f"{interval}min"
    )
    for i in range(len(bigjson)):
        bigjson[i]['datapoints']['limit'] = len(max_records)

    return bigjson


###########################################################
# Manual Tests (run with: python dendra_api_client.py)

def _run_tests():
    btime = True
    borg = False
    bstation = False
    bdatastream_id = False
    bdatapoints = True
    bdatapoints_lookup = False

    if btime:
        string_utc = '2019-03-01T08:00:00Z'
        print('UTC:', time_utc(string_utc))
        string_edt = '2019-03-01T08:00:00-0400'
        print('EDT:', time_utc(string_edt))
        string_hst = '2019-03-01T08:00:00HST'
        print('HST:', time_utc(string_hst))
        print('Empty (local default):', time_utc())

        tu = dt.datetime.strptime(string_utc, '%Y-%m-%dT%H:%M:%SZ')
        print('time_format utc:', time_format(tu, 'utc'))
        te = dt.datetime.strptime(string_edt, '%Y-%m-%dT%H:%M:%S%z')
        print('time_format edt:', time_format(te))
        print('time_format empty:', time_format())

    if borg:
        cdfw = get_organization_id('cdfw')
        print('List one Organization CDFW ID:', cdfw)
        erczo = list_organizations('erczo')
        print('List Organizations ERCZO ID:', erczo[0]['_id'])
        org_list = list_organizations()
        print('List All Organizations:')
        for org in org_list:
            print(org['_id'], org['name'])
        meta_erczo = get_meta_organization('erczo')
        print('Get metadata organization ERCZO slug:', meta_erczo)
        erczoid = get_organization_id('erczo')
        meta_erczo_id = get_meta_organization(orgid=erczoid)
        print('Get metadata organization ERCZO ID:', meta_erczo_id)

    if bstation:
        st_list = list_stations()
        print('\nALL Organization Stations\n', st_list)
        stslug = 'ucnrs'
        st_list = list_stations(stslug)
        print(f'\n{stslug.upper()} Stations\n')
        for station in st_list:
            print(station['_id'], station['name'], '\t', station['slug'])
        st_list = list_stations('Trump is Evil')
        print('\nBAD Organizations Stations\n', st_list)

    if bdatastream_id:
        airtemp_id = '5ae8793efe27f424f9102b87'
        airtemp_meta = get_meta_datastream_by_id(airtemp_id)
        print(airtemp_meta)
        airtemp_meta = get_meta_datastream_by_id(airtemp_id, {'$select[name]': 1})
        print(airtemp_meta)

    if bdatapoints:
        airtemp_id = '5ae8793efe27f424f9102b87'
        from_time = '2019-02-01T08:00:00Z'
        to_time = '2019-03-01T08:00:00Z'
        dd = get_datapoints(airtemp_id, from_time, to_time)
        dups = dd[dd.duplicated(keep=False)]
        print('get_datapoints count:', len(dd), 'min date:', dd.index.min(), 'max date:', dd.index.max())
        print('duplicates?\n', dups)
        dd = get_datapoints(airtemp_id, from_time)
        print('get_datapoints end date set to now, count:', len(dd))
        print(dd)

    if bdatapoints_lookup:
        station_id = '58e68cabdf5ce600012602b3'
        from_time = '2019-04-01T08:00:00Z'
        to_time = '2019-05-05T08:00:00Z'
        interval = 10
        tags = ['ds_Medium_Air', 'ds_Variable_Temperature', 'ds_Aggregate_Average']
        query = {
            'station_id': station_id,
            'time[$gte]': from_time,
            'time[$lt]': to_time,
            'tags': '.'.join(tags),
            '$sort[time]': 1,
            'time_local': 1,
            '$limit': 2000,
        }
        bigjson = lookup_datapoints(query, 'datapoints/lookup', interval)
        for doc in bigjson:
            print(doc['name'], len(doc['datapoints']['data']), doc['datapoints']['limit'], doc['_id'])


if __name__ == '__main__':
    _run_tests()
