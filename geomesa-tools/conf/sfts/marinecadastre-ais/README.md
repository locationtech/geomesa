# Marine Cadastre AIS

[Automated Identification System](https://en.wikipedia.org/wiki/Automatic_identification_system) is a global system to
track ships, with each vessel beaconing its position to other (nearby) vessels. While various commercial data providers 
exist, this converter is intended for data collected by the U.S. Coast Guard around the USA and openly published by 
[Marine Cadastre](https://marinecadastre.gov/ais/).

The data is disseminated as zipped ESRI File Geodatabases containing a number of tables. Only the `broadcast` table is 
considered here, which consists of vessel locations over time. Note that "Ship name and call sign fields have been 
removed, and the MMSI (Maritime Mobile Service Identity) field has been encrypted for the 2010 through 2014 data at the 
request of the U.S. Coast Guard." 
    
## Getting the Data

Machine-friendly links for bulk data download can be found 
[here](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2009/index.html). Replace the four digit year with that desired
 (2009-2016), data is then split into month and 
[UTM Zone](https://marinecadastre.gov/ais/AIS%20Documents/UTMZoneMap2014.png). Once data is downloaded, it must be 
unzipped e.g.
    
    find . -name "*.zip" -exec unzip {} \;

## Converting the Data

GeoMesa does not currently support ESRI File Geodatabases (FileGDB), so an external tool is required to convert the data 
into a suitable format. `ogr2ogr` from [GDAL](http://www.gdal.org/) can convert FileGDB into Comma Separated Value 
(CSV) format e.g.

    find . -name "*.gdb" -exec sh -c "ogr2ogr -f CSV /dev/stdout {} Broadcast -lco GEOMETRY=AS_XY | tail -n +2 > {}.csv" \;

Note that the `tail` command removes the header row from the output file, making ingest more amenable to processing 
using HDFS or similar. 

## Sample Ingest Command

Check that the `marinecadastre-ais` simple feature type and converter are available on the GeoMesa tools classpath. 
This is the default case. Note that you will need to use the command specific to your back-end e.g. `geomesa-accumulo`.

    geomesa env | grep 'marinecadastre-ais'

If they are not, merge the contents of `reference.conf` with `$GEOMESA_HOME/conf/application.conf`, or ensure that 
`reference.conf` is in `$GEOMESA_HOME/conf/sfts/marinecadastre-ais`.

To ingest using the GeoMesa command line interface:

    $ geomesa ingest -u username -c catalogName -s marinecadastre-ais -C marinecadastre-ais -t 8 /path/to/data/*.csv
    
Note this example uses 8 threads, which for all of the 2009 & 2010 data (approx 3.5B records) took 15h on a 5 node 
cluster.
