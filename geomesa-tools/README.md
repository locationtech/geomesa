# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full GeoMesa project from the GeoMesa source directory with 

    mvn clean install
    
You can also make the build process significantly faster by adding `-DskipTests`. This will create a file "geomesa-{version}-bin.tar.gz"
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
        create       Create a feature definition in a GeoMesa catalog
        delete       Delete a feature's data and definition from a GeoMesa catalog
        describe     Describe the attributes of a given feature in GeoMesa
        explain      Explain how a GeoMesa query will be executed
        export       Export a GeoMesa feature
        help         Show help
        ingest       Ingest a file of various formats into GeoMesa
        ingestRaster Ingest a raster file or raster files in a directory into GeoMesa
        list         List GeoMesa features for a given catalog
        tableconf    Perform table configuration operations
                
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
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
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

To install, copy the jai_core.jar and jai_code.jar into `$GEOMESA_HOME/lib/`

Optionally there is a script bundled as `$GEOMESA_HOME/bin/geomesa-install-jai` that will attempt to wget and install 
the jai libraries

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

#### Usage (required options denoted with star):
    $ geomesa help create
    Create a feature definition in a GeoMesa catalog
    Usage: create [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -dt, --dtg
           DateTime field name to use as the default dtg
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -sh, --shards
           Number of shards to use for the storage tables (defaults to number of
           tservers)
      * -s, --spec
           SimpleFeatureType specification
        -st, --use-shared-tables
           Use shared tables in Accumulo for feature storage (true/false)
           Default: true
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example command:
    geomesa create -u username -p password -c test_create -i instname -z zoo1,zoo2,zoo3 -fn testing -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 -dt dtg

### delete
To delete a feature on a specified catalog table, use the `delete` command.

####Usage (required options denoted with star):
    $ geomesa help delete
    Delete a feature's data and definition from a GeoMesa catalog
    Usage: delete [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -f, --force
           Force deletion of feature without prompt
           Default: false
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

    
####Example:
    geomesa delete -u username -p password -i instname -z zoo1,zoo2,zoo3 -c test_delete -fn testing

### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  

####Usage (required options denoted with star):
    $ geomesa help describe
    Describe the attributes of a given feature in GeoMesa
    Usage: describe [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one (true/false)
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

      
#### Example command:
    geomesa describe -u username -p password -c test_delete -fn testing
 
### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.

####Usage (required options denoted with star):
    $ geomesa help explain
    Explain how a GeoMesa query will be executed
    Usage: explain [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -q, --cql
           CQL predicate
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa explain -u username -p password -c test_catalog -fn test_feature -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  

####Usage (required options denoted with star):
    $ geomesa help export
    Export a GeoMesa feature
    Usage: export [options]
      Options:
        -at, --attributes
           Attributes from feature to export (comma-separated)...Comma-separated
           expressions with each in the format attribute[=filter_function_expression]|derived-attribute=filter_function_expression.
           filter_function_expression is an expression of filter function applied to attributes, literals and
           filter functions, i.e. can be nested
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -q, --cql
           CQL predicate
        -dt, --dt-attribute
           name of the date attribute to export
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -fmt, --format
           Format to export (csv|tsv|gml|json|shp|bin)
           Default: csv
        -id, --id-attribute
           name of the id attribute to export
        -i, --instance
           Accumulo instance name
        -lat, --lat-attribute
           name of the latitude attribute to export
        -lon, --lon-attribute
           name of the longitude attribute to export
        -lbl, --label-attribute
           name of the label attribute to export
        -max, --max-features
           Maximum number of features to return. default: Long.MaxValue
           Default: 2147483647
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -o, --output
           name of the file to output to instead of std out
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

<b>attribute expressions</b>
Attribute expressions are comma-separated expressions with each in the format 
    
    attribute[=filter_function_expression]|derived-attribute=filter_function_expression. 
    
`filter_function_expression` is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested

#### Example commands:
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "geom,text,user_name" -fmt csv -q "include" -m 100
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "geom,text,user_name" -fmt gml -q "user_name='JohnSmith'"
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "user_name,buf=buffer(geom\, 2)" \
        -fmt csv -q "[[ user_name like `John%' ] AND [ bbox(geom, 22.1371589, 44.386463, 40.228581, 52.379581, 'EPSG:4326') ]]"
    
### ingest
Ingests CSV, TSV, and SHP files from the local file system and HDFS. CSV and TSV files can be ingested either with explicit latitude and longitude columns or with a column of WKT geometries.
For lat/lon column ingest, the sft spec must include an additional geometry attribute in the sft beyond the number of columns in the file such as: `*geom:Point`.
The file type is inferred from the extension of the file, so ensure that the formatting of the file matches the extension of the file and that the extension is present.
*Note* the header if present is not parsed by Ingest for information, it is assumed that all lines are valid entries.

####Usage (required options denoted with star):
    $ geomesa help ingest
    Ingest a file of various formats into GeoMesa
    Usage: ingest [options] <file>...
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -cols, --columns
           the set of column indexes to be ingested, must match the
           SimpleFeatureType spec
        -dtf, --dt-format
           format string for the date time field
        -dt, --dtg
           DateTime field name to use as the default dtg
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -fmt, --format
           format of incoming data (csv | tsv | shp) to override file extension
           recognition
        -h, --hash
           flag to toggle using md5hash as the feature id
           Default: false
        -id, --id-fields
           the set of attributes to combine together to create a unique id for the
           feature (comma separated)
        -is, --index-schema
           GeoMesa index schema format string
        -i, --instance
           Accumulo instance name
        -lat, --lat-attribute
           name of the latitude field in the SimpleFeatureType if ingesting point
           data
        -lon, --lon-attribute
           name of the longitude field in the SimpleFeatureType if ingesting point
           data
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -sh, --shards
           Number of shards to use for the storage tables (defaults to number of
           tservers)
      * -s, --spec
           SimpleFeatureType specification
        -st, --use-shared-tables
           Use shared tables in Accumulo for feature storage (true/false)
           Default: true
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa ingest -u username -p password -c geomesa_catalog -fn somefeaturename -s fid:Double,dtg:Date,*geom:Geometry \
        --dtg dtg -dtf "MM/dd/yyyy HH:mm:ss" hdfs:///some/hdfs/path/to/file.csv
    
    geomesa ingest -u username -p password -c geomesa_catalog -a someAuths -v someVis -sh 42 -fn somefeaturename
     -s fid:Double,dtg:Date,lon:Double,lat:Double,*geom:Point -dt dtg -dtf "MM/dd/yyyy HH:mm:ss" 
     -id fid,dtg -h -lon lon -lat lat /some/local/path/to/file.tsv
     
    geomesa ingest -u username -p password -c test_catalog -f shapeFileFeatureName /some/path/to/file.shp

### ingestRaster
To ingest one or multiple raster image files into Geomesa, use the `ingestRaster` command. Input files, Geotiff or
DTED, are located on local file system. If chunking (only works for single file) is specified by option `-ck`,
input file is cut into chunks by size in kilobytes (option `-cs or --chunk-size`) and chunks are ingested. Ingestion
is done in local or remote mode (by option `-m or --mode`, default is local). In local mode, files are ingested
directly from local host into Accumulo tables. In remote mode, raster files are serialized and stored in a HDFS
directory from where they are ingested. Optionally, ingested raster files as a coverage can be registered to a
GeoServer by option `-g or --geoserver-config`.

*Note:* Make sure GDAL is installed when doing chunking that depends on GDAL utility `gdal_translate`.

*Note:* It assumes input raster files have CRS set to EPSG:4326. For non-EPSG:4326 files, they need to be converted into
EPSG:4326 raster files before ingestion. An example of doing conversion with GDAL utility is `gdalwarp -t_srs EPSG:4326
input_file out_file`.

####Usage (required options denoted with star):
    $ geomesa help ingestRaster
    Ingest a raster file or files in a directory into GeoMesa
    Usage: ingestRaster [options]
      Options:
        -a, --auths
           Accumulo authorizations
        -ck, --chunk
           Create raster chunks before ingestion
           Default: false
        -cs, --chunk-size
           Desired size (in kilobytes) of each chunk
           Default: 600
      * -f, --file
           Single raster file or directory of raster files to be ingested
        -fmt, --format
           Format of incoming raster data (geotiff | DTED) to override file
           extension recognition
        -g, --geoserver-config
           Geoserver configuration info
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           (true/false)
           Default: false
        -m, --mode
           Ingestion mode (local | remote, default to local)
           Default: local
        -par, --parallel-level
           Maximum number of local threads for ingesting multiple raster files
           (default to 1)
           Default: 1
        -p, --password
           Accumulo password (will prompt if not supplied)
        -qt, --query-threads
           Threads for quering raster data
      * -t, --raster-table
           Accumulo table for storing raster data
        -sh, --shards
           Number of shards to use for the storage tables (defaults to number of
           tservers)
        -tm, --timestamp
           Ingestion time (default to current time)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -wm, --write-memory
           Memory allocation for ingestion operation
        -wt, --write-threads
           Threads for writing raster data
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa ingestRaster -u username -p password -t geomesa_raster -f /some/local/path/to/raster.tif

    geomesa ingestRaster -u username -p password -t geomesa_raster -ck -cs 1000 -m remote -f /some/path/to/raster.tif

    geomesa ingestRaster -u username -p password -t geomesa_raster -fmt DTED -par 8 -f /some/local/path/to/raster_dir
     -g "user=USERNAME,password=PASS,url=http://GEOSERVER:8080/geoserver,namespace=RASTER"

### list
To list the features on a specified catalog table, use the `list` command.  

####Usage (required options denoted with star):
    $ geomesa help list
    List GeoMesa features for a given catalog
    Usage: list [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa list -u username -p password -c test_catalog
    
### tableconf
To list, describe, and update the configuration parameters on a specified table, use the `tableconf` command. 

####Usage (required options denoted with star):
    $ geomesa help tableconf
    Perform table configuration operations
    Usage: tableconf [options] [command] [command options]
      Commands:
        list      List the configuration parameters for a geomesa table
          Usage: list [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        describe      Describe a given configuration parameter for a table
          Usage: describe [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            *     --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        update      Update a given table configuration parameter
          Usage: update [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            * -n, --new-value
                 New value of the property
            *     --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa tableconf list -u username -p password -c test_catalog -fn test_feature -t st_idx
    geomesa tableconf describe -u username -p password -c test_catalog -fn test_feature -t attr_idx --param table.bloom.enabled
    geomesa tableconf update -u username -p password -c test_catalog -fn test_feature -t records --param table.bloom.enabled -n true

