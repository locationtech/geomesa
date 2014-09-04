# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full GeoMesa project with `mvn clean install`. This will build the project and geomesa-tools JAR file.  
 
GeoMesa Tools relies on a GEOMESA_HOME environment variable. In your `~/.bashrc`, add:

    export GEOMESA_HOME=/path/to/geomesa/source/directory
    export PATH=${GEOMESA_HOME}/geomesa-tools/bin:$PATH

Don't forget to source `~/.bashrc`. Also make sure that $ACCUMULO_HOME and $HADOOP_HOME are set. For your convenience, you can also run:
    
    . /path/to/geomesa/source/dir/geomesa-tools/bin/geomesa configure

Make sure to include the `. ` prefix to the command, as this sources your new environment variables.  

Now, you should be able to use GeoMesa from any directory on your computer. To test, `cd` to a different directory and run:

    geomesa

This should print out the following usage text: 

    GeoMesa Tools 1.0
    Required for each command:
            -u, --username: the Accumulo username : required
    Optional parameters:
            -p, --password: the Accumulo password. This can also be provided after entering a command.
            help, -help, --help: show this help dialog or the help dialog for a specific command (e.g. geomesa create help)
    Supported commands are:
             create: Create a feature in GeoMesa
             delete: Delete a feature from the specified Catalog Table in GeoMesa
             describe: Describe the attributes of a specified feature
             explain: Explain and plan a query in GeoMesa
             export: Export all or a set of features in csv, geojson, gml, or shp format
             ingest: Ingest a feature into GeoMesa
             list: List the features in the specified Catalog Table
             tableconf: List, describe, and update table configuration parameters
                
This usage text gives a brief overview of how to use each command, and this is expanded upon below with example commands.
The command line tools also provides help for each command by passing `--help` to any individual command.  

The Accumulo username and password is required for each command. Specify the username and password
in each command by using `-u` or `--username `and `-p` or `--password` respectively. One can also only specify the username
on the command line using `-u` or `--username` and type the password in an additional prompt, where the password will be
hidden from the shell history.

A test script is included under `geomesa\bin` that runs each command provided by geomesa-tools. Edit this script
by including your Accumulo username, password, test catalog table, test feature name, and test SFT specification. Then,
run the script from the command line to ensure there are no errors in the output text. 

## Command Explanations and Usage

### create
To create a new feature on a specified catalog table, use the `create` command.  
#### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `-f` or `--feature-name`.  
Specify the SimpleFeatureType schema with `-s` or `--spec`.  
Specify the default temporal attribute with `-d` or `--default-date`.
#### Example command:
    geomesa create -u username -p password -c test_create -f testing -s id:String:index=true,dtg:Date,geom:Point:srid=4326

### delete
To delete a feature on a specified catalog table, use the `delete` command.  
#### Required flags: 
Specify the catalog table to use with `-o` or `--catalog`. NOTE: Catalog tables will not be deleted when using the `delete` command, only the tables related to the given feature.  
Specify the feature to delete with `-f` or `--feature-name`.  
#### Example command:
    geomesa delete -u username -p password  -c test_delete -f testing

### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  
#### Required flags: 
Specify the catalog table to use with `-o` or `--catalog`.
Specify the feature to describe with `-f` or `--feature-name`.  
#### Optional flags: 
To pipe the output of the command, use `-q` or `--quiet`.
#### Example command:
    geomesa describe -u username -p password -c test_delete -f testing
 
### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.
#### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `-f` or `-feature-name`.  
Specify the filter string with `-q` or `--filter`.
#### Example command:
    geomesa explain -u username -p password -c geomesa_catalog -f twittersmall -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  
#### Required flags:
Specify the catalog table to use with `-c` or `--catalog`.  
Specify the feature to export with `-f` or `--feature-name`.  
Specify the export format with `-o` or `--format`. The supported export formats are csv, gml, geojson, and shp.
#### Optional flags:
To retrieve specific attributes from each feature, use `-a` or `--attributes` followed by a String of comma-separated attribute names.  
To set a maximum number of features to return, use `-m` or `--maxFeatures` followed by the maximum number of features.  
To run an ECQL query, use `-q` or `--query` followed by the query filter string.  
To export to stdOut, use `-s` or `--stdout`. This is useful for piping output.
#### Example commands:
    geomesa export -u username -p password -c geomesa_catalog -f twittersmall -a "geom,text,user_name" -o csv -q "include" -m 100  
    geomesa export -u username -p password -c geomesa_catalog -f twittersmall -a "geom,text,user_name" -o gml -q "user_name='JohnSmith'"
           
### ingest
Ingests CSV, TSV, and SHP files from the local file system and HDFS. CSV and TSV files can be ingested either with explicit latitude and longitude columns or with a column of WKT geometries.
For lat/lon column ingest, the sft spec must include an additional geometry attribute in the sft beyond the number of columns in the file such as: `*geom:Point`.
The file type is inferred from the extension of the file, so ensure that the formatting of the file matches the extension of the file and that the extension is present.
*Note* the header if present is not parsed by Ingest for information, it is assumed that all lines are valid data entries.

#### Usage
    geomesa ingest -u username -p password -c geomesa_catalog -f somefeaturename -s fid:Double,dtg:Date,*geom:Geometry 
    --datetime dtg --dtformat "MM/dd/yyyy HH:mm:ss" --file hdsf:///some/hdfs/path/to/file.csv
    
    geomesa ingest -u username -p password -c geomesa_catalog  -a someAuths -v someVis --shards 42 -f somefeaturename
     -s fid:Double,dtg:Date,lon:Double,lat:Double,*geom:Point --datetime dtg --dtformat "MM/dd/yyyy HH:mm:ss" 
     --idfields fid,dtg --hash --lon lon --lat lat --file /some/local/path/to/file.tsv
     
    geomesa ingest -u username -p password -c geomesa_catalog -f shapeFileFeatureName --file /some/path/to/file.shp

with the following parameters:
 
`-c` or `--catalog` The accumulo table name, the table will be created if not already extant.

`-a` or `--auths` The optional accumulo authorizations to use.

`-v` or `--visibilities` The optional accumulo visibilities to use. 

`-i` or `--indexSchemaFormat` The optional accumulo index schema format to use. If not set Ingest defaults to the Default SpatioTemporal Schema in the AccumuloDataStore class.
This option and the `--shards` cannot be provided together and Ingest exits if this occurs.

`--shards` The optional number of shards to use for this catalog. If catalog exists Ingest defaults to number of shards used in extant schema. 
This option and the `--indexSchemaFormat` cannot be provided together and Ingest exits if this occurs.

`-f` or `--feature-name` The name of the SimpleFeatureType to be used.

`-s` or `--sftspec` The SimpleFeatureType of the CSV or TSV file, this must match exactly with the number and order of columns and data formats in the file being ingested and must also include a default geometry field.
If attempting to ingest files with explicit latitude and longitude columns, the sft spec must include an additional attribute beyond the number of columns in the file such as: `*geom:Point` in order for it to work.

`--datetime` The optional name of the field in the SFT specification that corresponds to the the *time* column.

`--dtformat` The optional Joda DateTimeFormat string for the date-time field, e.g.: "MM/dd/yyyy HH:mm:ss". This must be surrounded by quotes and must match exactly the format in the source file. 
If a invalid dtformat is given Ingest attempts to parse the date-time value using the ISO8601 standard.

`--idfields` The optional comma separated list of ID fields used to generate the feature IDs. If empty, it is assumed that the ID will be generated via a hash on all attributes of that line.

`-h` or `--hash` The optional flag to hash the value of the id generated from idfields for the feature IDs.

`--lon` The optional name of the longitude field. This field is not required for ingesting WKT geometries.

`--lat` The optional name of the latitude field. This field is not required for ingesting WKT geometries.

`--file` The file path or hdfs path to the csv file or tsv file being ingested.

### list
To list the features on a specified catalog table, use the `list` command.  
#### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`.
#### Optional flags:
To pipe the output of the command, use `-q` or `--quiet`.
#### Example command:
    geomesa list -u username -p password -c geomesa_catalog
    
### tableconf
To list, describe, and update the configuration parameters on a specified table, use the `tableconf` command.  
### Subcommands
#### list
List all table configuration parameters
##### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`.
Specify the feature name with `-f` or `--feature-name`.
Specify the table suffix (attr_idx, st_idx, or records) with `-s` or `--suffix`. 
#### describe
Describe a single table configuration parameter.
##### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`.
Specify the feature name with `-f` or `--feature-name`.
Specify the table configuration parameter with `--param`.
Specify the table suffix (attr_idx, st_idx, or records) with `-s` or `--suffix`.
#### update
Update a single table configuration parameter to a new value.
##### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`.
Specify the feature name with `-f` or `--feature-name`.
Specify the table configuration parameter with `--param`.
Specify the new value for the configuration parameter with `-n` or `--new-value`.
Specify the table suffix (attr_idx, st_idx, or records) with `-s` or `--suffix`.

#### Example commands:
    geomesa tableconf list -u username -p password -c geomesa_catalog -f twittersmall -s st_idx
    geomesa tableconf describe -u username -p password -c geomesa_catalog -f twittersmall --param table.bloom.enabled -s attr_idx
    geomesa tableconf update -u username -p password -c geomesa_catalog -f twittersmall --table.bloom.enabled -n true -s records
    