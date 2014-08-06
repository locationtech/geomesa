# GeoMesa Tools

## Introduction
Geomesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full Geomesa project with `mvn clean install`. This will build the project and geomesa-tools JAR file.  
 
Geomesa Tools relies on environment variables to connect to the Accumulo and Zookeeper instances. To set these, edit your `~/.bashrc` file and 
add the following lines at the end, adding the information specific to your instance:  

    export GEOMESA_USER=Your.user.here  
    export GEOMESA_PASSWORD=Your.password.here  
    export GEOMESA_INSTANCEID=Your.instanceId.here  
    export GEOMESA_ZOOKEEPERS=Your.zookeepers.string.here
   
Source your `~/.bashrc` file by running `source ~/.bashrc`. After this step, you will likely need to log out and log back in for your 
environment variables to be set correctly.  

Next, run the command line tools jar with the following command from your root Geomesa directory:  

    java -jar geomesa-tools/target/geomesa-tools-accumulo1.5-1.0.0-SNAPSHOT-shaded.jar --help

This should print out the following usage text:

    GeoMesa Tools 1.0
    Usage: geomesa-tools [export|list|explain|delete|create|ingest] [options]
     --help
           show help command
    Command: export [options]
    Export all or a set of features in csv, geojson, gml, or shp format
     --catalog <value>
           the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data
     --typeName <value>
           the name of the feature to export
     --format <value>
           the format to export to (e.g. csv, tsv)
     --attributes <value>
           attributes to return in the export
     --idAttribute <value>
           feature ID attribute to query on
     --latAttribute <value>
           latitude attribute to query on
     --lonAttribute <value>
           longitude attribute to query on
     --dateAttribute <value>
           date attribute to query on
     --maxFeatures <value>
           max number of features to return
     --query <value>
           ECQL query to run on the features
    Command: list [options]
    List the features in the specified Catalog Table
     --catalog <value>
           the name of the Accumulo table to use
    Command: explain [options]
    Explain and plan a query in Geomesa
     --catalog <value>
           the name of the Accumulo table to use
     --typeName <value>
           the name of the new feature to be create
     --filter <value>
           the filter string
    Command: delete [options]
    Delete a feature from the specified Catalog Table in Geomesa
     --catalog <value>
           the name of the Accumulo table to use
     --typeName <value>
           the name of the new feature to be create
    Command: create [options]
    Create a feature in Geomesa
      --catalog <value>
            the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data
      --typeName <value>
            the name of the new feature to be create
      --sft <value>
            the string representation of the SimpleFeatureType
    Command: ingest [options]
    Ingest a feature into GeoMesa
      --file <value>
            the file you wish to ingest, e.g.: ~/capelookout.csv
      --format <value>
            the format of the file, it must be csv or tsv
      --table <value>
            the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data
      --typeName <value>
            the name of the feature type to be ingested
      -s <value> | --spec <value>
            the sft specification for the file
      --datetime <value>
            the name of the datetime field in the sft
      --dtformat <value>
            the format of the datetime field
            
This usage text gives a brief overview of how to use each command, and this is expanded upon below with example commands.  
Note that each command should be prefixed by `java -jar geomesa-tools/target/geomesa-tools-accumulo1.5-1.0.0-SNAPSHOT-shaded.jar` on the command line.

## Command Explanations and Usage

### export
To export features, use the `export` command.  
#### Required flags:
Specify the catalog table to use with `--catalog`.  
Specify the feature to export with `--typeName`.  
Specify the export format with `--format`. The supported export formats are csv, gml, geojson, and shp.
#### Optional flags:
To retrieve specific attributes from each feature, use `--attributes` followed by a String of comma-separated attribute names.  
To set a maximum number of features to return, use `--maxFeatures` followed by the maximum number of features.  
To run an ECQL query, use `--query` followed by the query filter string.  
#### Example commands:
    export --catalog geomesa_catalog --typeName twittersmall --attributes "geom,text,user_name" --format csv --query "include" --maxFeatures 1000  
    export --catalog geomesa_catalog --typeName twittersmall --attributes "geom,text,user_name" --format gml --query "user_name='JohnSmith'"
        
### list
To list the features on a specified catalog table, use the `list` command.  
#### Required flags: 
Specify the catalog table to use with `--catalog`
#### Example command:
    list --catalog geomesa_catalog

### create
To create a new feature on a specified catalog table, use the `create` command.  
#### Required flags: 
Specify the catalog table to use with `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `--typeName`.  
Specify the SimpleFeatureType schema with `--sft`.  
#### Example command:
    create --catalog test_create --typeName testing --sft id:String:indexed=true,dtg:Date,geom:Point:srid=4326
        
### delete
To delete a feature on a specified catalog table, use the `delete` command.  
#### Required flags: 
Specify the catalog table to use with `--catalog`. NOTE: Catalog tables will not be deleted when using the `delete` command, only the tables related to the given feature.  
Specify the feature to delete with `--typeName`.  
#### Example command:
    delete --catalog test_delete --typeName testing
  
### ingest
Ingests TSV and CSV files containing WKT geometries with the following caveat:CSV files must surround values with double quotation marks, e.g.: `"37266103","2013-07-17","POINT(0.0 0.0)"` the first and last quotation marks are optional however. Also the WKT Geometry is assumed to be the last column of the CSV/TSV file.
#### Usage
    ingest --file <> --format <> --table <> --typeName <> --spec <> --datetime <> --dtformat <>

note: *the `<>` marks are where user values would go*

with the following parameters:
     
`--file` The file path to the csv file or tsv file being ingested.

`--format` The format of that file, either CSV or TSV.

`--table` The accumulo table name, the table will be created if not already extant.

`--typeName` The name of the SimpleFeatureType to be used.

`--spec` The SimpleFeatureType of the CSV or TSV file, must match the layout of columns in the CSV/TSV file

`--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.

`--dtformat` The Joda DateTimeFormat string for the date-time field, e.g.: "MM/dd/yyyy HH:mm:ss"


### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.
#### Required flags: 
Specify the catalog table to use with `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `--typeName`.  
Specify the filter string with `--filter`.
#### Example command:
    explain --catalog geomesa_catalog --typeName twittersmall --filter "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
