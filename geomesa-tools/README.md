# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full GeoMesa project from the GeoMesa source directory with 

    mvn clean install
    
You can also make the build process significantly faster by adding `-DskipTests`. This will create a file "geomesa-bin.tar.gz" 
in the geomesa-assemble/target directory. Untar this file with

    tar xvfz geomesa-assemble/target/geomesa-${version}-bin.tar.gz
    
Next, `cd` into the newly created directory with
    
    cd geomesa-${version}

GeoMesa Tools relies on a GEOMESA_HOME environment variable. Running
    
    . bin/geomesa configure

with the `. ` prefix will set this for you, add $GEOMESA_HOME/bin to your PATH, and source your new environment variables in your current shell session.

Now, you should be able to use GeoMesa from any directory on your computer. To test, `cd` to a different directory and run:

    geomesa

This should print out the following usage text: 

    bash$ geomesa
    Usage: geomesa [command] [command options]
      Commands:
        tableconf    Perform table configuration operations
        list         List GeoMesa features for a given catalog
        export       Export a GeoMesa feature
        delete       Delete a feature's data and definition from a GeoMesa catalog
        describe     Describe the attributes of a given feature in GeoMesa
        ingest       Ingest a file of various formats into GeoMesa
        create       Create a feature definition in a GeoMesa catalog
        explain      Explain how a GeoMesa query will be executed
        help         Show help
                
This usage text lists the available commands. To see help for an individual command run `geomesa help <command-name>` which for example
will give you something like this:

    bash$ geomesa help list
    List GeoMesa features for a given catalog
    Usage: list [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
            --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
      * -p, --password
           Accumulo password
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

The Accumulo username and password is required for each command. Specify the username and password
in each command by using `-u` or `--username `and `-p` or `--password`, respectively. One can also only specify the username
on the command line using `-u` or `--username` and type the password in an additional prompt, where the password will be
hidden from the shell history.

A test script is included under `geomesa\bin\geomesa-test-script.sh` that runs each command provided by geomesa-tools. Edit this script
by including your Accumulo username, password, test catalog table, test feature name, and test SFT specification. Default values
are already included in the script. Then, run the script from the command line to ensure there are no errors in the output text. 

In all commands below, one can add `--instance-name`, `--zookeepers`, `--auths`, and `--visibilities` (or in short form `-i, -z, -a, -v`) arguments
to properly configure the Accumulo data store connector. The Accumulo instance name and Zookeepers string can usually
be automatically assigned as long as Accumulo is configured correctly. The Auths and Visibilities strings will have to
be added as arguments to each command, if needed.

###Enabling Shape File Support
Due to licensing restrictions, a necessary dependency (jai-core) for shape file support must be manually installed:
    
    <dependency>
      <groupId>javax.media</groupId>
      <artifactId>jai_core</artifactId>
      <version>1.1.3</version>
    </dependency>
    
This library can be downloaded from your local nexus repo or `http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib.zip`

To install, copy the jai_core.jar and jai_code.jar into $GEOMESA_HOME/lib/

###Logging configuration
GeoMesa tools comes bundled by default with an slf4j implementation that is installed to the $GEOMESA_HOME/lib directory
 named `slf4j-log4j12-1.7.5.jar` If you already have an slf4j implementation installed on your Java Classpath you may
 see errors at runtime and will have to exclude one of the jars. This can be done by simply renaming the bundled
 `slf4j-log4j12-1.7.5.jar` file to `slf4j-log4j12-1.7.5.jar.exclude`
 
Note that if no slf4j implementation is installed you will see this error:

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

## Command Explanations and Usage

### create
To create a new feature on a specified catalog table, use the `create` command.  
#### Options (required options denoted with star)
      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
      --dtg                 DateTime field name to use as the default dtg
    * -f, --feature-name    Simple Feature Type name on which to operate
      -i, --instance        Accumulo instance name
      --mock                Run everything with a mock accumulo instance instead of a real one, Default: false
    * -p, --password        Accumulo password
      --shards              Number of shards to use for the storage tables (defaults to number of tservers)
    * -s, --spec            SimpleFeatureType specification
      --useSharedTables     Use shared tables in Accumulo for feature storage Default: true
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host:port, comma separated)


#### Example command:
    geomesa create -u username -p password -c test_create -f testing -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 --dtg dtg

### delete
To delete a feature on a specified catalog table, use the `delete` command.

####Options (required options denoted with star):
      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
    * -f, --feature-name    Simple Feature Type name on which to operate
      --force               Force deletion of feature without prompt, Default: false
      -i, --instance        Accumulo instance name
      --mock                Run everything with a mock accumulo instance instead of a real one, Default: false
    * -p, --password        Accumulo password
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host:port, comma separated)
    
####Example:
    geomesa delete -u username -p password  -c test_delete -f testing

### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  

####Options (required options denoted with star)
      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
    * -f, --feature-name    Simple Feature Type name on which to operate
      -i, --instance        Accumulo instance name      
      --mock                Run everything with a mock accumulo instance instead of a real one     Default: false
    * -p, --password        Accumulo password
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host:port, comma separated)
      
#### Example command:
    geomesa describe -u username -p password -c test_delete -f testing
 
### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.

####Options (required options denoted with star)
      -a, --auths            Accumulo authorizations
    * -c, --catalog          Catalog table name for GeoMesa
    *     --cql              CQL predicate
    * -f, --feature-name     Simple Feature Type name on which to operate
      -i, --instance         Accumulo instance name      
      --mock                 Run everything with a mock accumulo instance instead of a real one     Default: false
    * -p, --password         Accumulo password
    * -u, --user             Accumulo user name
      -v, --visibilities     Accumulo scan visibilities
      -z, --zookeepers       Zookeepers (host:port, comma separated)

#### Example command:
    geomesa explain -u username -p password -c test_catalog -f test_feature -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  

      --attrs               Attribute expression from feature to export (comma-separated)...see notes for syntax
      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
      -q, --cql             CQL predicate
      -dtg                  name of the date attribute to export
    * -f, --feature-name    Simple Feature Type name on which to operate
      --file                name of the file to output to instead of std out
      -o, --format          Format to export (csv|tsv|gml|json|shp) Default: csv
      -id                   name of the id attribute to export
      -i, --instance        Accumulo instance name
      -lat                  name of the latitude attribute to export
      -lon                  name of the longitude attribute to export
      -m, --maxFeatures     Maximum number of features to return. default: Long.MaxValue     Default: 2147483647
      --mock                Run everything with a mock accumulo instance instead of a real one     Default: false
    * -p, --password        Accumulo password
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host:port, comma separated)

<b>attribute expressions</b>
Attribute expressions are comma-separated expressions with each in the format 
    
    attribute[=filter_function_expression]|derived-attribute=filter_function_expression. 
    
`filter_function_expression` is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested

#### Example commands:
    geomesa export -u username -p password -c test_catalog -f test_feature --attrs "geom,text,user_name" -o csv -q "include" -m 100
    geomesa export -u username -p password -c test_catalog -f test_feature --attrs "geom,text,user_name" -o gml -q "user_name='JohnSmith'"
    geomesa export -u username -p password -c test_catalog -f test_featurel --attrs "user_name,buf=buffer(geom\, 2)"
    -o csv -q "[[ user_name like `John%' ] AND [ bbox(geom, 22.1371589, 44.386463, 40.228581, 52.379581, 'EPSG:4326') ]]"
    
### ingest
Ingests CSV, TSV, and SHP files from the local file system and HDFS. CSV and TSV files can be ingested either with explicit latitude and longitude columns or with a column of WKT geometries.
For lat/lon column ingest, the sft spec must include an additional geometry attribute in the sft beyond the number of columns in the file such as: `*geom:Point`.
The file type is inferred from the extension of the file, so ensure that the formatting of the file matches the extension of the file and that the extension is present.
*Note* the header if present is not parsed by Ingest for information, it is assumed that all lines are valid entries.

#### Usage
    geomesa ingest -u username -p password -c geomesa_catalog -f somefeaturename -s fid:Double,dtg:Date,*geom:Geometry 
    --dtg dtg --dtformat "MM/dd/yyyy HH:mm:ss" --file hdsf:///some/hdfs/path/to/file.csv
    
    geomesa ingest -u username -p password -c geomesa_catalog  -a someAuths -v someVis --shards 42 -f somefeaturename
     -s fid:Double,dtg:Date,lon:Double,lat:Double,*geom:Point --datetime dtg --dtformat "MM/dd/yyyy HH:mm:ss" 
     --idfields fid,dtg --hash --lon lon --lat lat --file /some/local/path/to/file.tsv
     
    geomesa ingest -u username -p password -c test_catalog -f shapeFileFeatureName --file /some/path/to/file.shp

with the following parameters:
 
      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
      --cols, --columns     the set of column indexes to be ingested, must match the SimpleFeatureType spec
      --dtg                 DateTime field name to use as the default dtg
      --dtFormat            format string for the date time field
    * -f, --feature-name    Simple Feature Type name on which to operate
      --format              format of incoming data (csv | tsv | shp) to override file extension recognition
      --hash                flag to toggle using md5hash as the feature id     Default: false
      --idFields            the set of attributes to combine together to create a unique id for the feature (comma separated)
      --indexSchema         GeoMesa index schema format string
      -i, --instance        Accumulo instance name
      --lat                 name of the latitude field in the SimpleFeatureType if ingesting point     data
      --lon                 name of the longitude field in the SimpleFeatureType if ingesting point data
      --mock                Run everything with a mock accumulo instance instead of a real one Default: false
    * -p, --password        Accumulo password
      --shards              Number of shards to use for the storage tables (defaults to number of tservers)
    * -s, --spec            SimpleFeatureType specification
      --useSharedTables     Use shared tables in Accumulo for feature storage (default false) Default: true
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host[:port], comma separated 

### list
To list the features on a specified catalog table, use the `list` command.  
#### Usage:

      -a, --auths           Accumulo authorizations
    * -c, --catalog         Catalog table name for GeoMesa
      -i, --instance        Accumulo instance name
      --mock                Run everything with a mock accumulo instance instead of a real one     Default: false
    * -p, --password        Accumulo password
    * -u, --user            Accumulo user name
      -v, --visibilities    Accumulo scan visibilities
      -z, --zookeepers      Zookeepers (host:port, comma separated)

#### Example command:
    geomesa list -u username -p password -c test_catalog
    
### tableconf
To list, describe, and update the configuration parameters on a specified table, use the `tableconf` command. 

####Usage

    list      List the configuration parameters for a geomesa table
      Usage: list [options]
      Options:
        -a, --auths             Accumulo authorizations
      * -c, --catalog           Catalog table name for GeoMesa
      * -f, --feature-name      Simple Feature Type name on which to operate
        -i, --instance          Accumulo instance name        
        --mock                  Run everything with a mock accumulo instance instead of a real one Default: false
      * -p, --password          Accumulo password
      * -t, --table-suffix      Table suffix to operate on (attr_idx, st_idx, or records)
      * -u, --user              Accumulo user name
        -v, --visibilities      Accumulo scan visibilities
        -z, --zookeepers        Zookeepers (host:port, comma separated)
    
    describe      Describe a given configuration parameter for a table
      Usage: describe [options]
      Options:
        -a, --auths             Accumulo authorizations
      * -c, --catalog           Catalog table name for GeoMesa
      * -f, --feature-name      Simple Feature Type name on which to operate
        -i, --instance          Accumulo instance name        
        --mock                  Run everything with a mock accumulo instance instead of a real one Default: false
      * --param                 Accumulo table configuration param name (e.g. table.bloom.enabled)
      * -p, --password          Accumulo password
      * -t, --table-suffix      Table suffix to operate on (attr_idx, st_idx, or records)
      * -u, --user              Accumulo user name
        -v, --visibilities      Accumulo scan visibilities
        -z, --zookeepers        Zookeepers (host:port, comma separated)
    
    update      Update a given table configuration parameter
      Usage: update [options]
      Options:
        -a, --auths             Accumulo authorizations
      * -c, --catalog           Catalog table name for GeoMesa
      * -f, --feature-name      Simple Feature Type name on which to operate
        -i, --instance          Accumulo instance name        
        --mock                  Run everything with a mock accumulo instance instead of a real one Default: false
      * -n, --new-value         New value of the property
      * --param                 Accumulo table configuration param name (e.g. table.bloom.enabled)
      * -p, --password          Accumulo password
      * -t, --table-suffix      Table suffix to operate on (attr_idx, st_idx, or records)
      * -u, --user              Accumulo user name
        -v, --visibilities      Accumulo scan visibilities
        -z, --zookeepers        Zookeepers (host:port, comma separated)


#### Example commands:
    geomesa tableconf list -u username -p password -c test_catalog -f test_feature -s st_idx
    geomesa tableconf describe -u username -p password -c test_catalog -f test_feature --param table.bloom.enabled -s attr_idx
    geomesa tableconf update -u username -p password -c test_catalog -f test_feature --table.bloom.enabled -n true -s records

