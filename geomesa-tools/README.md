# GeoMesa Tools

## Introduction
Geomesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full Geomesa project with `mvn clean install`. This will build the project and geomesa-tools JAR file.  
 
Geomesa Tools relies on a GEOMESA_HOME environment variable. In your `~/.bashrc`, add:

    export GEOMESA_HOME=/path/to/root/geomesa/directory
    export PATH=${GEOMESA_HOME}/bin:$PATH

Don't forget to source `~/.bashrc`. Also make sure that $ACCUMULO_HOME and $HADOOP_HOME are set.  

Now, you should be able to use Geomesa from any directory on your computer. To test, `cd` to a different directory and run:

    geomesa

This should print out the following usage text: 

    Geomesa Tools 1.0
    Required for each command:
            -u, --username: the Accumulo username : required
    Optional parameters:
            -p, --password: the Accumulo password. This can also be provided after entering a command.
            help, -help, --help: show this help dialog or the help dialog for a specific command (e.g. geomesa create help)
    Supported commands are:
             create: Create a feature in Geomesa
             delete: Delete a feature from the specified Catalog Table in Geomesa
             describe: Describe the attributes of a specified feature
             explain: Explain and plan a query in Geomesa
             export: Export all or a set of features in csv, geojson, gml, or shp format
             ingest: Ingest a feature into GeoMesa
             list: List the features in the specified Catalog Table
            
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
Specify the feature to create with the `-f` or `--feature_name`.  
Specify the SimpleFeatureType schema with `-s` or `--spec`.  
Specify the default temporal attribute with `-d` or `--default_date`.
#### Example command:
    geomesa create -u username -p password -c test_create -f testing -s id:String:index=true,dtg:Date,geom:Point:srid=4326

### delete
To delete a feature on a specified catalog table, use the `delete` command.  
#### Required flags: 
Specify the catalog table to use with `-o` or `--catalog`. NOTE: Catalog tables will not be deleted when using the `delete` command, only the tables related to the given feature.  
Specify the feature to delete with `-f` or `--feature_name`.  
#### Example command:
    geomesa delete -u username -p password  -c test_delete -f testing

### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  
#### Required flags: 
Specify the catalog table to use with `-o` or `--catalog`.
Specify the feature to describe with `-f` or `--feature_name`.  
#### Example command:
    geomesa describe -u username -p password -c test_delete -f testing
 
### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.
#### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`. This can be a previously created catalog table, or a new catalog table.  
Specify the feature to create with the `-f` or `-feature_name`.  
Specify the filter string with `-q` or `--filter`.
#### Example command:
    geomesa explain -u username -p password -c geomesa_catalog -f twittersmall -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  
#### Required flags:
Specify the catalog table to use with `-c` or `--catalog`.  
Specify the feature to export with `-f` or `--feature_name`.  
Specify the export format with `-o` or `--format`. The supported export formats are csv, gml, geojson, and shp.
#### Optional flags:
To retrieve specific attributes from each feature, use `-a` or `--attributes` followed by a String of comma-separated attribute names.  
To set a maximum number of features to return, use `-m` or `--maxFeatures` followed by the maximum number of features.  
To run an ECQL query, use `-q` or `--query` followed by the query filter string.  
#### Example commands:
    geomesa export -u username -p password -c geomesa_catalog -f twittersmall -a "geom,text,user_name" -o csv -q "include" -m 100  
    geomesa export -u username -p password -c geomesa_catalog -f twittersmall -a "geom,text,user_name" -o gml -q "user_name='JohnSmith'"
           
### ingest
Ingests TSV and CSV files containing WKT geometries with the following caveat:CSV files must surround values with double quotation marks, e.g.: `"37266103","2013-07-17","POINT(0.0 0.0)"` the first and last quotation marks are optional however. Also the WKT Geometry is assumed to be the last column of the CSV/TSV file.
#### Usage
    geomesa ingest -u username -p password --file <> --format <> --table <> --feature_name <> --spec <> --datetime <> --dtformat <>

note: *the `<>` marks are where user values would go*

with the following parameters:
     
`--file` The file path to the csv file or tsv file being ingested.

`--format` The format of that file, either CSV or TSV.

`--table` The accumulo table name, the table will be created if not already extant.

`--feature_name` The name of the SimpleFeatureType to be used.

`--spec` The SimpleFeatureType of the CSV or TSV file, must match the layout of columns in the CSV/TSV file

`--datetime` The name of the field in the SFT spec above that corresponds to the the *time* column in the data being ingested.

`--dtformat` The Joda DateTimeFormat string for the date-time field, e.g.: "MM/dd/yyyy HH:mm:ss"

### list
To list the features on a specified catalog table, use the `list` command.  
#### Required flags: 
Specify the catalog table to use with `-c` or `--catalog`
#### Example command:
    geomesa list -u username -p password -c geomesa_catalog
    