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
            the name of the Accumulo table to use
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
            the name of the new feature to be created
      --filter <value>
            the filter string
    Command: delete [options]
    Delete a feature from the specified Catalog Table in Geomesa
      --catalog <value>
            the name of the Accumulo table to use
      --typeName <value>
            the name of the new feature to be created
    Command: create [options]
    Create a feature in Geomesa
      --catalog <value>
            the name of the Accumulo table to use
      --typeName <value>
            the name of the new feature to be created
      --sft <value>
            the string representation of the SimpleFeatureType
    Command: ingest [csv|tsv]
    Ingest a file into GeoMesa
    Command: ingest csv [options]
    Ingest a csv file
      --file <value>
            the file you wish to ingest, e.g.: ~/capelookout.csv
      --catalog <value>
            the name of the Accumulo table to use
      --typeName <value>
            the name of the feature type to be ingested
      -s <value> | --spec <value>
            the sft specification for the file
      --datetime <value>
            the name of the datetime field in the sft
      --dtformat <value>
            the format of the datetime field
      --skip-header <value>
            specify if to skip the header or not (false includes the header)
      
      The following named parameters are optional for cases when ingesting csv/tsv files 
      containing explicit latitude and longitude columns, which will be constructed into point data.
      
      --idfields <value>
            the comma separated list of id fields used to generate the feature ids. 
            if empty it is assumed that the id will be generated via a hash on the attributes of that line
      --lon <value>
            the name of the longitude field
      --lat <value>
            the name of the latitude field
            
    Command: ingest tsv [options]
    Ingest a tsv file
      --file <value>
            the file you wish to ingest, e.g.: ~/capehatteras.tsv
      --catalog <value>
            the name of the Accumulo table to use
      --typeName <value>
            the name of the feature type to be ingested
      -s <value> | --spec <value>
            the sft specification for the file
      --datetime <value>
            the name of the datetime field in the sft
      --dtformat <value>
            the format of the datetime field
      --skip-header <value>
            specify if to skip the header or not (false includes the header)
      
      The following named parameters are optional for cases when ingesting csv/tsv files 
      containing explicit latitude and longitude columns, which will be constructed into point data.
      
      --idfields <value>
            the comma separated list of id fields used to generate the feature ids. 
            if empty it is assumed that the id will be generated via a hash on the attributes of that line
      --lon <value>
            the name of the longitude field
      --lat <value>
            the name of the latitude field
            
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
Ingests files into GeoMesa.

#### Sub Commands:
The following sub commands correspond to the file type you are attempting to ingest. The required flags used depend on the sub command.

#### csv
 For ingesting csv files containing WKT geometries in the last column. WKT geometries containing commas must be surrounded by quotes e.g.: `37266103,2013-07-17,"LINESTRING (30 10, 10 30, 40 40)"`
 
###### csv required flags:
         
    `--file` The file path to the csv file being ingested.
    
    `--catalog` The accumulo table name, the table will be created if not already extant.
    
    `--typeName` The name of the SimpleFeatureType to be used.
    
    `--spec` The SimpleFeatureType of the CSV file, must match the layout of columns in the CSV file.
    
    `--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.
    
    `--dtformat` The Joda DateTimeFormat string for the date-time field, e.g.: "MM/dd/yyyy HH:mm:ss"
    
optional flags for 

     `--skip-header` true|false indicate if you wish to skip the first line of the file or not.
    
optional flags for ingesting csv files with explicit latitude and longitude columns (non-WKT geometry), both `lon` and `lat` fields must be present to be valid
    
    `--idfields` the comma separated list of id fields used to generate a feature ids.
    
    `--lon` the name of the longitude field, must match the name in the sft.
    
    `--lat` the name of the latitude field, must match the name in the sft.

#### tsv
 For ingesting tsv files containing WKT geometries in the last column.
 
###### tsv required flags:
         
    `--file` The file path to the tsv file being ingested.
    
    `--catalog` The accumulo table name, the table will be created if not already extant.
    
    `--typeName` The name of the SimpleFeatureType to be used.
    
    `--spec` The SimpleFeatureType of the TSV file, must match the layout of columns in the TSV file.
    
    `--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.
    
    `--dtformat` The Joda DateTimeFormat string for the date-time field, e.g.: "MM/dd/yyyy HH:mm:ss"
    
optional flags for 

     `--skip-header` true|false indicate if you wish to skip the first line of the file or not.
    
optional flags for ingesting tsv files with explicit latitude and longitude columns (non-WKT geometry), both `lon` and `lat` fields must be present to be valid
    
    `--idfields` the comma separated list of id fields used to generate a feature ids.
    
    `--lon` the name of the longitude field, must match the name in the sft.
    
    `--lat` the name of the latitude field, must match the name in the sft.

#### Example Ingest commands:
   
    ingest csv --file capefear.csv --catalog outerbanks --typeName cape
                --spec id:Double,time:Date,*geom:Geometry --datetime time --dtformat MM/dd/yyyy
                
    ingest tsv --file capehatteras.tsv --catalog outerbanks --typeName cape
                --spec id:Double,time:Date,*geom:Geometry --datetime time --dtformat MM/dd/yyyy
                
    ingest csv --file capelookout.csv --catalog outerbanks --typeName cape
                --spec id:Double,time:Date,lat:Double,lon:Double,*geom:Point --datetime time --dtformat MM/dd/yyyy
                --skip-header true --idfields id,time,lat,lon --lon lon --lat lat

### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.
#### Required flags: 
Specify the catalog table to use with `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `--typeName`.  
Specify the filter string with `--filter`.
#### Example command:
    explain --catalog geomesa_catalog --typeName twittersmall --filter "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
    