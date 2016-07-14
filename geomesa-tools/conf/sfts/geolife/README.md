## GeoLife GPS Trajectory Data for GeoMesa

This directory provides [GeoLife](http://research.microsoft.com/en-us/projects/geolife/) GeoMesa ingest commands and converter configuration files.

The GeoLife dataset contain timestamped latitude, longitude, and altitude for 182 different users. There are 17,621 total points, spanning 1.2 kilometers and years 2007-2012.

Data was collected from users' phones and other GPS loggers and is meant to model activity such as commuting, shopping, sightseeing, etc.

Some user entries contain additional information about the mode of transportation.

This readme describes the full process from original source data to GeoMesa ingest. 

## Getting GeoLife data

The GeoLife data set can be downloaded using the provided ```download-data.sh``` script in `$GEOMESA_HOME/bin/` as such

    ./download-data.sh geolife

Alternatively, download the GeoLife Data [here](http://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/). Each folder of the dataset represents one user's GPS logs. Each log is formatted as a PLT file as follows (based on the user guide contained in the zip file):

> Line 1...6 are useless in this dataset, and can be ignored. Points are described in following lines, one for each line.

> Field 1: Latitude in decimal degrees.  
> Field 2: Longitude in decimal degrees.  
> Field 3: All set to 0 for this dataset.  
> Field 4: Altitude in feet (-777 if not valid).  
> Field 5: Date - number of days (with 
fractional part) that have passed since 12/30/1899.  
> Field 6: Date as a string.  
> Field 7: Time as a string.  
> Note that field 5 and field 6&7 represent the same date/time in this dataset. You may use either of them.  
> Example: 
> 39.906631,116.385564,0,492,40097.5864583333,2009-10-11,14:04:30
> 39.906554,116.385625,0,492,40097.5865162037,2009-10-11,14:04:35

More information on features of the GeoLife data, see their documentation [here] (http://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/) as well as the user guide contained in the zip of the dataset.


## Ingest Commands

Check that `geolife` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep geolife
    
If it is not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that `reference.conf` is in `$GEOMESA_HOME/conf/sfts/geolife` 
 

Run the ingest. You may optionally point to a different accumulo instance using `-i` and `-z` options. See `geomesa help ingest` for more detail.

    geomesa ingest -u USERNAME -c CATALOGNAME -s geolife -C geolife useridFolder/Trajectory/trackid.plt

**Note:** Pay special attention to the directory structure of the data file. The converter expects this hierarchy in order to correctly parse the user ID and the track ID of the trace. 

Microsoft Research asks that you cite the following papers when using this dataset
>
[1] Yu Zheng, Lizhu Zhang, Xing Xie, Wei-Ying Ma. Mining interesting locations and travel sequences from GPS trajectories. In Proceedings of International conference on World Wild Web (WWW 2009), Madrid Spain. ACM Press: 791-800. [2] Yu Zheng, Quannan Li, Yukun Chen, Xing Xie, Wei-Ying Ma. Understanding Mobility Based on GPS Data. In Proceedings of ACM conference on Ubiquitous Computing (UbiComp 2008), Seoul, Korea. ACM Press: 312-321. [3] Yu Zheng, Xing Xie, Wei-Ying Ma, GeoLife: A Collaborative Social Networking Service among User, location and trajectory. Invited paper, in IEEE Data Engineering Bulletin. 33, 2, 2010, pp. 32-40.
>

Further be aware that any errors in ingestion will be logged to `$GEOMESA_HOME/logs`
