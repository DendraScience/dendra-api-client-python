# dendra-api-client-python
Helper functions for accessing  Dendra metadata and downloading large datasets    
Dendra API enforces a 2000 record paging limit on datapoint requests.  This library automates paging through large datasets until you get the entire record set.      
Various accessor functions make pulling metadata easier.   
In general, the following are true:    
- GET_META functions will pull all the metadata available for one thing.  They have options for refining the returned metadata.    
- LIST functions will provide the name and ID of all available things in a category.
- GET_DATAPOINTS pulls actual data.       
-- get_datapoints() is your basic request. Requires a Mongo ID (You can find these on datastreams at https://dendra.science) and a start and end date.   
-- get_datapoints_from_station_id() will pull all datastreams associated with a single station.        
-- get_datapoints_from_id_list() will pull all datastreams listed in an array.   

