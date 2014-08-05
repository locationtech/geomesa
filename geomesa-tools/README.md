# GeoMesa Tools

The command line tools project for GeoMesa.

## General Usage

## export
To export features, use the ```export``` command.  
###Required flags:
Specify the catalog table to use with ```--catalog```.  
Specify the feature to export with ```--typeName```.  
Specify the export format with ```--format```. The supported export formats are csv, gml, geojson, and shp.
###Optional flags:
To retrieve specific attributes from each feature, use ```--attributes``` followed by a String of comma-separated attribute names.  
To set a maximum number of features to return, use ```--maxFeatures``` followed by the maximum number of features.  
To run an ECQL query, use ```--query``` followed by the query filter string.  
###Example commands:
```export --catalog geomesa_catalog --typeName twittersmall --attributes "geom,text,user_name" --format csv --query "include" --maxFeatures 1000```  
```export --catalog geomesa_catalog --typeName twittersmall --attributes "geom,text,user_name" --format gml --query "user_name='Meghan Ho'"```
        
## list
To list the features on a specified catalog table, use the ```list``` command.  
###Required flags: 
Specify the catalog table to use with ```--catalog```
###Example command:
```list --catalog geomesa_catalog```

## create
To create a new feature on a specified catalog table, use the ```create``` command.  
###Required flags: 
Specify the catalog table to use with ```--catalog```. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the ```--typeName```.  
Specify the SimpleFeatureType schema with ```--sft```.  
###Example command:
```create --catalog test_jdk2pq_create --typeName testing --sft id:String:indexed=true,dtg:Date,geom:Point:srid=4326```
        
## delete
To delete a feature on a specified catalog table, use the ```delete``` command.  
###Required flags: 
Specify the catalog table to use with ```--catalog```. NOTE: Catalog tables will not be deleted when using the ```delete``` command, only the tables related to the given feature.  
Specify the feature to delete with ```--typeName```.  
###Example command:
```delete --catalog test_jdk2pq_create --typeName testing```
  
## ingest

Ingests TSV and CSV files containing WKT geometries with the following caveat:CSV files must surround values with double quotation marks, e.g.: `"37266103","2013-07-17","POINT(0.0 0.0)"` the first and last quotation marks are optional however. Also the WKT Geometry is assumed to be the last column of the CSV/TSV file.
####Usage
   
```ingest --file <> --format <> --table <> --typeName <> --spec <> --datetime <> --dtformat <> --method <>```

note: *the `<>` marks are where user values would go*

with the following parameters:
     
`--file` The file path to the csv file or tsv file being ingested.

`--format` The format of that file, either CSV or TSV.

`--table` The accumulo table name, the table will be created if not already extant.

`--spec` The SimpleFeatureType of the CSV or TSV file, must match the layout of columns in the CSV/TSV file

`--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.

`--method` The method of choice to perform the ingest, currently only `naive` is supported