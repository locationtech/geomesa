## How to use csv upload from cURL

# Upload a CSV file to the csv endpoint and get a UUID in response
    > curl -F csvfile=@test.csv <GEOSERVER_ROOT>/geomesa/csv

    c0c16ad7-5707-428b-9a28-d3f8aca86589

# check inferred name and type schema
    > curl <GEOSERVER_ROOT>/geomesa/csv/c0c16ad7-5707-428b-9a28-d3f8aca86589.csv/types
    
    test3772907466880411288
    lat:Double, lon:Double, time:String

# POST to shapefile URL to create. Can override name and schema, and optionally specify latitude and longitude field names 
    > curl -X POST --data "latField=lat&lonField=lon&schema=lat%3ADouble%2C%20lon%3ADouble%2C%20time%3ADate%2C*%20geometry%3APoint%3Asrid%3D4326%3Aindex%3Dtrue" <GEOSERVER_ROOT>/geomesa/csv/c0c16ad7-5707-428b-9a28-d3f8aca86589.shp
    
    c0c16ad7-5707-428b-9a28-d3f8aca86589.shp

# Shapefile is available at specified URL
    > curl <GEOSERVER_ROOT>/geomesa/csv/c0c16ad7-5707-428b-9a28-d3f8aca86589.shp > test.zip

# Delete CSV and shapefile when they're no longer needed
    > curl -X DELETE <GEOSERVER_ROOT>/geomesa/csv/c0c16ad7-5707-428b-9a28-d3f8aca86589.csv
    > curl <GEOSERVER_ROOT>/geomesa/csv/c0c16ad7-5707-428b-9a28-d3f8aca86589.csv/types
    
    java.util.NoSuchElementException: key not found: c0c16ad7-5707-428b-9a28-d3f8aca86589
