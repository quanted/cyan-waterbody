### Web API

#### Waterbody Aggregation
Initiate waterbody aggregation, after image files in mounted directory:
```
http://127.0.0.1:8080/waterbody/aggregate/?year=2021&day=88
```
Year and day of year: 2021 and 88.
Defaults to daily data.
```
http://127.0.0.1:8080/waterbody/aggregate/?year=2021&day=88&weekly=True
```
Year and day of the START of the 7 day period for weekly data: 2021 and 88. When weekly parameter is present and set to True, will aggregate weekly data.

#### Waterbody ID search
To search for a waterbody given a latitude/longitude point lat/lng:
```
http://127.0.0.1:8080/waterbody/search/?lat=26.959&lng=-80.869
```
Will return json with OBJECTID for the waterbody that contains the point lat/lng, will return "NA" if no waterbody found.

Process queries the database for all waterbody geometry bounding boxes, WaterbodyBounds table, then for each waterbody bounds that contains the point a geometry polygon contains point operation is performed.

##### Failed Aggregation
If a waterbody aggregation fails, the details are logged in the DailyStatus and WeeklyStatus tables. Those failed year, yday, objectid, type attempts can be retried by:
```
http://127.0.0.1:8080/waterbody/aggregate/retry/
```
Successfully processed waterbodies will have a status of "PROCESSED", failed will be "FAILED". The status is automatically updated if successful.
All "FAILED"s are retried.

##### Data Retrieval
Waterbody aggregation data can be retrieved using:
```
http://127.0.0.1:8080/waterbody/data/?OBJECTID=6624886
```
Defaults to daily data, and will retrieve all data for the objectid in the database (if objectid is valid).

Optional parameters

| parameter | type | description |
|-----------|-------|------------------|
| start_year | int | Beginning year for the data to be returned |
| start_day | int | Beginning day of the start year for the data to be returned |
| end_year | int | End year for the date to be returned |
| end_day | int | End day of the end year for the data to be returned |
| daily | bool | Retrieving daily or weekly data, weekly if daily=False |
| ranges | json list | List of pairs, corresponding to pixel values and their groupings. example = "[[0:10],[11:100],[101:200],[201:255]]"|

No combination is enforced, so yearly, monthly or other seasonality-focused time periods can be requested.


### Volumes
Three volumes are required for the process to function, can be found in docker-compose.yml:
1. database, the directory containing the sqlite database.
2. images, the directory containing all tif images (both weekly and daily)
3. geometry, the directory containing the geometry shapefiles for the waterbody polygons.
Each volume path is managed by an env variable, also found in docker-compose.yml


### CLI 

All the same functionality is accessible directly through the command line interface using:
```
python main.py -h
```
Assuming CWD is the root of the source files, where main.py is located. Help and command documentation is provided through the CLI.

