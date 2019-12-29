'''
Dendra API Query

Author: Collin Bode
Date: 2019-05-12

Purpose: 
Simplifies pulling data from the https://dendra.science time-series data management system.
Dendra API requires paging of records in sets of 2,016.  This library performs
that function automatically. 

NOTE: the 'get_datapoints' function, which is the primary reason for this library is quite slow. It will
be replaced in the next version when we have min.io set up on the server to handle very large requests.

Parameters:
    query: a JSON object with the tags, organization, stations, and start/end times
    endpoint: what API endpoint to query. 'datapoints/lookup' (default), 'station','datastream','datapoint'
    interval: datalogger minutes between records, integer. 5 = ERCZO (default), 10 = UCNRS, 15 = USGS
'''

import requests
import json
import pandas as pd
import datetime as dt
import pytz
from dateutil import tz
from dateutil.parser import parse
from getpass import getpass
import concurrent.futures

# Params
#url = 'https://api.edge.dendra.science/v1/'  # version 1 of the API has been deprecated
url = 'https://api.edge.dendra.science/v2/'
headers = {"Content-Type":"application/json"}

# Time Helper Functions
# These apply standardized formating and UTC conversion
def time_utc(str_time=""):
    if(str_time == ""):
        dt_time = dt.datetime.now(pytz.utc)
    else:
        dt_time = parse(str_time)
        if(dt_time.tzinfo != pytz.utc):
            dt_time = dt_time.astimezone(pytz.utc)
    return dt_time

def time_format(dt_time=dt.datetime.now()):
     str_time = dt.datetime.strftime(dt_time,"%Y-%m-%dT%H:%M:%S") # "%Y-%m-%dT%H:%M:%S.%f"
     return str_time

def authenticate(email):
    data = {
        'email': email,
        'strategy': 'local',
        'password': getpass()
    }
    r = requests.post(url+'authentication', json=data)
    assert r.status_code == 201
    token = r.json()['accessToken']
    headers['Authorization'] = token
    

# List Functions help find what you are looking for, do not retreive full metadata
def list_organizations(orgslug='all'):
    # options: 'erczo','ucnrs','chi'
    query = {
        '$sort[name]': 1,
        '$select[name]':1,
        '$select[slug]':1
    }
    if(orgslug != 'all'):
        query['slug'] = orgslug
    
    r = requests.get(url + 'organizations', headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    return rjson['data']    

def list_stations(orgslug='all',query_add='none'):
    # orgslug options: 'erczo','ucnrs','chi'
    # NOTE: can either do all orgs or one org. No option to list some,
    #       unless you custom add to the query.
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        '$select[slug]': 1,
        '$limit': 2016
    }

    # Narrow query to one organization
    if(orgslug != 'all'):
        org_list = list_organizations(orgslug)
        if(len(org_list) == 0): 
            return 'ERROR: no organizations found with that acronym.'
        orgid = org_list[0]['_id'] 
        query['organization_id'] = orgid

    # Modify query adding custom elements
    if(query_add != 'none'):
        for element in query_add:
            query[element] = query_add[element]

    # Request JSON from Dendra         
    r = requests.get(url + 'stations', headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    return rjson['data']

def list_datastreams_by_station_id(station_id,query_add = ''):
    query = {
        '$sort[name]': 1,
        '$select[name]': 1,
        'station_id': station_id,
        '$limit': 2016
    }
    if(query_add != ''):
        query.update(query_add)    

    # Request JSON from Dendra         
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    return rjson['data']
    
# translate SensorDB to Dendra ID
def get_datastream_id_from_dsid(dsid,orgslug='all',station_id = ''):
    # Legacy SensorDB used integer DSID (DatastreamID).  
    # This is a helper function to translate between Dendra datastream_id's and DSID's
    query = {'$limit':2016}

    # Narrow query to one station
    if(station_id != ''):
        query.update({'station_id':station_id})

    # Narrow query to one org or loop through all organizations
    org_list = list_organizations(orgslug)
    if(len(org_list) == 0): 
        print('ERROR: no organizations found with that acronym.')
        return ''
    # Build list of metadata 
    bigjson = {'data':[]}
    for org in org_list:
        orgid = org['_id']
        orgname = org['name']
        #print(orgname,orgid,query)
        query_org = query
        query_org.update({'organization_id': orgid})
        r = requests.get(url + 'datastreams', headers=headers, params=query)
        assert r.status_code == 200
        rjson = r.json()
        if(len(rjson['data']) > 0):
            bigjson['data'].extend(rjson['data'])
            #print(orgname,len(rjson['data']))
    dsid_list = []
    for ds in bigjson['data']:
        #print(ds['name'],ds['_id'])
        if('external_refs' not in ds):
            continue
        for ref in ds['external_refs']:
            if(ref['type'] == 'odm.datastreams.DatastreamID'):
                #print("\t",ref['type'], ref['identifier'])
                dsid_list.append([ref['identifier'],ds['_id']])
    for row in dsid_list:
        int_dsid = int(row[0])
        datastream_id = row[1]
        if(dsid == int_dsid):
            #print('FOUND!',dsid,int_dsid,datastream_id)
            return datastream_id


# GET Metadata returns full metadata
def get_datastream_by_id(datastream_id,query_add = ''): 
    # deprecated use get_meta_
    rjson = get_meta_datastream_by_id(datastream_id,query_add)
    return rjson
    
def get_meta_datastream_by_id(datastream_id,query_add = ''):
    query = { '_id': datastream_id }
    if(query_add != ''):
        query.update(query_add)
    r = requests.get(url + 'datastreams', headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    return rjson['data'][0]   

def get_meta_station_by_id(station_id,query_add = ''):
    query = { '_id': station_id }
    if(query_add != ''):
        query.update(query_add)
    r = requests.get(url + 'stations', headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    return rjson['data'][0]   


# GET Datapoints returns actual datavalues for only one datastream.  
# Returns a Pandas DataFrame columns. Both local and UTC time will be returned.
# Parameters: time_end is optional. Defaults to now. time_type is optional default 'local', either 'utc' or 'local' 
# if you choose 'utc', timestamps must have 'Z' at the end to indicate UTC time.

def get_datapoints(datastream_id,time_start,time_end=time_format(),time_type='local'):
    if(time_type == 'utc' and time_end[-1] != 'Z'):
        time_end += 'Z'
        
    query = {
        'datastream_id': datastream_id,
        'time[$gt]': time_start,
        'time[$lt]': time_end,
        '$sort[time]': "1",
        '$limit': "2016"
    } 
    if(time_type != 'utc'): 
        query.update({ 'time_local': "true" })
    
    # Dendra requires paging of 2,000 records maximum at a time.
    # To get around this, we loop through multiple requests and append
    # the results into a single dataset.
    try:
        r = requests.get(url + 'datapoints', headers=headers, params=query)
        assert r.status_code == 200
    except:
        return r.status_code
    rjson = r.json()
    bigjson = rjson
    while(len(rjson['data']) > 0):
        df = pd.DataFrame.from_records(bigjson['data'])
        time_last = df['lt'].max()
        query['time[$gt]'] = time_last
        r = requests.get(url + 'datapoints', headers=headers, params=query)
        assert r.status_code == 200
        rjson = r.json()
        bigjson['data'].extend(rjson['data'])

    # Create Pandas DataFrame and set time as index
    # If the datastream has data for the time period, populate DataFrame
    if(len(bigjson['data']) > 0):
        df = pd.DataFrame.from_records(bigjson['data'])
    else:
        df = pd.DataFrame(columns={'lt','t','v'})
        
    # Get human readable name for data column
    datastream_meta = get_meta_datastream_by_id(datastream_id,{'$select[name]':1,'$select[station_id]':1})
    station_meta = get_meta_station_by_id(datastream_meta['station_id'],{'$select[slug]':1})
    stn = station_meta['slug'].replace('-',' ').title().replace(' ','')
    datastream_name = stn+'_'+datastream_meta['name'].replace(' ','_')
    
    # Rename columns, then set index to timestamp local or utc 
    df.rename(columns={'lt':'timestamp_local','t':'timestamp_utc','v':datastream_name},inplace=True)
    if(time_type == 'utc'):
        df.set_index('timestamp_utc', inplace=True, drop=True)  
    else:
        df.set_index('timestamp_local', inplace=True, drop=True)

    # Return DataFrame
    return df

# GET Datapoints from List returns a dataframe of datapoints from a list of datastream ids. The function is 
# threaded for speed.  List must be an array of text variables which are datastream ids.  The first datastream
# on the list will create the time-index, so it is best if this one is the most complete of the list. If it has 
# time gaps, the rest of the dataframe can be comprimised.  This may need to be changes in the future.
# All requirements of above get_datapoints apply to get_datapoints_from_list.
def get_datapoints_from_id_list(datastream_id_list,time_start,time_end=time_format(),time_type='local'):
    i = 0
    boo_new = True
    dftemp_list = [] # list of dataframes from the results

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for dsid in datastream_id_list:
            i += 1
            future = executor.submit(get_datapoints,dsid,time_start,time_end,'local')
            dftemp_list.append(future)

        for future in concurrent.futures.as_completed(dftemp_list):
            dftemp = future.result()
            #print(dftemp.columns)

            # Check to see if any datapoints were returned.  
            # Many datastreams are not functional for the desired time frame.
            # If none, then skip the datastream and continue
            if(len(dftemp) == 0):
                    print(i,dftemp.columns[1],'No values, skipping...') 
                    continue
            # If there are datapoints, check to see if the dataframe has been created yet. 
            # If not, create, if so, add another column
            if(boo_new == True):
                df = dftemp
                boo_new = False
                print(i,dftemp.columns[1],'NEW dataframe created!')
            else:
                dftemp.drop(dftemp.columns[0],axis=1,inplace=True)
                df = df.merge(dftemp,how="left",left_index=True,right_index=True)
                print(i,dftemp.columns[0],'added.')
    return df


# Lookup is an earlier attempt. Use get_datapoints unless you have to use this.    
def __lookup_datapoints_subquery(bigjson,query,endpoint='datapoints/lookup'):
    r = requests.get(url + endpoint, headers=headers, params=query)
    assert r.status_code == 200
    rjson = r.json()
    if(len(bigjson) == 0): # First pull assigns the metadata 
        bigjson = rjson
    else:  # all others just add to the datapoints
        for i in range(0,len(bigjson)):
            bigjson[i]['datapoints']['data'].extend(rjson[i]['datapoints']['data'])
    return bigjson

def lookup_datapoints(query,endpoint='datapoints/lookup',interval=5):    
    # Determine start and end timestamps
    # Start time
    #time_start_original = dt.datetime.strptime(query['time[$gte]'],'%Y-%m-%dT%H:%M:%SZ')
    time_start_original = parse(query['time[$gte]'])
    #time_start_original = pytz.utc.localize(time_start_original)
    # end time
    if('time[$lt]' in query):
        #time_end_original = dt.datetime.strptime(query['time[$lt]'],'%Y-%m-%dT%H:%M:%SZ')
        time_end_original = parse(query['time[$lt]'])
        #time_end_original = pytz.utc.localize(time_end_original)
    else: 
        time_end_original_local = dt.datetime.now(tz.tzlocal())
        time_end_original = time_end_original_local.astimezone(pytz.utc)
    
    # Paging limit: 2016 records. 
    interval2k = (dt.timedelta(minutes=interval) * 2016 )

    # Perform repeat queries until the time_end catches up with the target end date
    time_start = time_start_original
    time_end = time_start_original+interval2k
    bigjson = {}
    while(time_end < time_end_original and time_start < time_end_original):    
        bigjson = __lookup_datapoints_subquery(bigjson,query,endpoint)
        time_start = time_end
        time_end = time_start+interval2k 
    # One final pull after loop for the under 2016 records left
    bigjson = __lookup_datapoints_subquery(bigjson,query,endpoint)

    # Count total records pulled and update limit metadata
    max_records = pd.date_range(start=time_start_original,end=time_end_original, tz='UTC',freq=str(interval)+'min')
    for i in range(0,len(bigjson)):
        bigjson[i]['datapoints']['limit'] = len(max_records) 

    # return the full metadata and records
    return bigjson


###############################################################################
# Unit Tests
#
def __main():
    btime = False
    borg = False
    bstation = False
    bdatastream_id = False
    bdatapoints = False
    bdatapoints_lookup = False    

    ####################
    # Test Time
    if(btime == True):
        # time_utc converts string to datetime
        string_utc = '2019-03-01T08:00:00Z'
        print('UTC:',time_utc(string_utc))
        string_edt = '2019-03-01T08:00:00-0400'
        print('EDT:',time_utc(string_edt))
        string_hst = '2019-03-01T08:00:00HST'
        print('HST:',time_utc(string_hst))
        print('Empty (local default):',time_utc())
        
        # time_format converts datetime to utc string
        tu = dt.datetime.strptime(string_utc,'%Y-%m-%dT%H:%M:%SZ')
        print('time_format utc:',time_format(tu))
        te = dt.datetime.strptime(string_edt,'%Y-%m-%dT%H:%M:%S%z')
        print('time_format edt:',time_format(te))
        print('time_format empty:',time_format())
    
    
    ####################
    # Test Organizations
    if(borg == True):
        # Get One Organization ID
        erczo = list_organizations('erczo')
        print('Organizations ERCZO ID:',erczo[0]['_id'])
        
        # Get All Organization IDs        
        org_list = list_organizations()
        print('All Organizations:')
        print("ID\t\t\tName")
        for org in org_list:
            print(org['_id'],org['name'])
        
        # Send a BAD Organization slug
        orgs = list_organizations('Trump_is_Evil')
        print('BAD Organizations:',orgs)
    
    ####################    
    # Test stations
    if(bstation == True):
        # Get All stations
        st_list = list_stations()
        print('\nALL Organization Stations\n',st_list)
        
        # Get Stations from UCNRS only
        stslug = 'ucnrs'
        st_list = list_stations(stslug)
        #print(st_erczo)    
        print('\n',stslug.upper(),'Stations\n')
        print("ID\t\t\tName\t\tSlug")
        for station in st_list:
            print(station['_id'],station['name'],"\t",station['slug'])
        
        # Modify Query
        query_add = {'$select[station_type]':1}
        print(query_add)
        st_list = list_stations(stslug) #,query_add)
        print('\n',stslug.upper(),'Stations with station_type added\n',st_list)    
    
        # What happens when you send a BAD organization string?
        st_list = list_stations('Trump is Evil')
        print('\nBAD Organizations Stations\n',st_list)
     
    ####################    
    # Test Datastream from id
    if(bdatastream_id == True):
        # Get all Metadata about one Datastream 'South Meadow WS, Air Temp C'        
        airtemp_id = '5ae8793efe27f424f9102b87'
        airtemp_meta = get_meta_datastream_by_id(airtemp_id)
        print(airtemp_meta)
        
        # Get only Name from Metadata using query_add
        airtemp_meta = get_meta_datastream_by_id(airtemp_id,{'$select[name]':1})
        print(airtemp_meta)
                
    ####################        
    # Test Datapoints 
    if(bdatapoints == True):
        airtemp_id = '5ae8793efe27f424f9102b87'
        from_time = '2019-02-01T08:00:00.000Z' # UTC, not local PST time
        to_time = '2019-03-01T08:00:00Z'
        #to_time = None
        dd = get_datapoints(airtemp_id,from_time,to_time)
        dups = dd[dd.duplicated(keep=False)]
        print('get_datapoints count:',len(dd),'min date:',dd.index.min(),'max date:',dd.index.max())
        print('duplicates?\n',dups)
        
        # No end date
        to_time = None
        dd = get_datapoints(airtemp_id,from_time)
        print('get_datapoints end date set to now, count:',len(dd),'min date:',dd.index.min(),'max date:',dd.index.max())
        print(dd)
        
    ####################        
    # Test Datapoints Lookup 
    if(bdatapoints_lookup == True):
        # Parameters
        orgid = '58db17c424dc720001671378' # ucnrs
        station_id = '58e68cabdf5ce600012602b3'
        from_time = '2019-04-01T08:00:00.000Z' # UTC, not local PST time
        to_time = '2019-05-05T08:00:00Z'
        interval = 10 # 5,10,15
        
        tags = [
            'ds_Medium_Air',
            'ds_Variable_Temperature',
            'ds_Aggregate_Average'
        ]
        query = {
            'station_id': station_id,
            'time[$gte]': from_time,
            'tags': '.'.join(tags),
            '$sort[time]': 1,
            'time_local': 1,
            '$limit': 2000
        }
        if('to_time' in locals()):
        	query['time[$lt]'] = to_time
        #print(query)
        # Test the Query
        bigjson = lookup_datapoints(query,'datapoints/lookup',interval)
        
        # Show the results
        for doc in bigjson:
            print(doc['name'],len(doc['datapoints']['data']),doc['datapoints']['limit'],doc['_id'])

if(__name__ == '__main__'):
    __main()