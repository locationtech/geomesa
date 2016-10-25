# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Installation
To begin using the command line tools, first build the full GeoMesa project from the GeoMesa source directory with 

    mvn clean install
    
You can also make the build process significantly faster by adding `-DskipTests`. This will create a file called ``geomesa-tools-{version}-bin.tar.gz``
in the ``geomesa-tools/target`` directory. Untar this file with

    tar xvfz geomesa-tools/target/geomesa-tools-${version}-bin.tar.gz
    
Next, `cd` into the newly created directory with
    
    cd geomesa-tools-${version}

GeoMesa Tools relies on a GEOMESA_HOME environment variable. Running
    
    source bin/geomesa configure

with the `source` shell function will set this for you, add $GEOMESA_HOME/bin to your PATH, and set your new environment variables in your current shell session. Now you should be able to use GeoMesa from any directory on your computer.

Note: The tools will read the ACCUMULO_HOME and HADOOP_HOME environment variables to load the appropriate JAR files for Hadoop, Accumulo, Zookeeper, and Thrift. If possible, we recommend installing the tools on the Accumulo master server, as you may also need various configuration files from Hadoop/Accumulo in order to run certain commands. Use the ``geomesa classpath`` command in order to see what JARs are being used.

If you are running the tools on a system without Accumulo installed and configured, the ``install-hadoop-accumulo.sh`` script in the ``bin`` directory may be used to download the needed Hadoop/Accumulo JARs into the ``lib`` directory. You should edit this script to match the versions used by your installation. 

To test, `cd` to a different directory and run:

    geomesa

This should print out the following usage text: 

    $ geomesa
    Usage: geomesa [command] [command options]
      Commands:
        add-attribute-index    Run a Hadoop map reduce job to add an index for attributes
        add-index              Add or update indices for an existing GeoMesa feature type
        config-table           Perform table configuration operations
        create-schema          Create a GeoMesa feature type
        delete-catalog         Delete a GeoMesa catalog completely (and all features in it)
        delete-features        Delete features from a table in GeoMesa. Does not delete any tables or schema information.
        delete-raster          Delete a GeoMesa Raster table
        env                    Examine the current GeoMesa environment
        explain                Explain how a GeoMesa query will be executed
        export                 Export features from a GeoMesa data store
        export-bin             Export features from a GeoMesa data store in a binary format.
        gen-avro-schema        Generate an Avro schema from a SimpleFeatureType
        get-names              List GeoMesa feature types for a given catalog
        get-schema             Describe the attributes of a given GeoMesa feature type
        get-sft-config                Get the SimpleFeatureType of a feature
        help                   Show help
        ingest                 Ingest/convert various file formats into GeoMesa
        ingest-raster          Ingest raster files into GeoMesa
        keywords               Add/Remove/List keywords on an existing schema
        query-raster-stats     Export queries and statistics about the last X number of queries to a CSV file.
        remove-schema          Remove a schema and associated features from a GeoMesa catalog
        stats-analyze          Analyze statistics on a GeoMesa feature type
        stats-bounds           View or calculate bounds on attributes in a GeoMesa feature type
        stats-count            Estimate or calculate feature counts in a GeoMesa feature type
        stats-histogram        View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values
        stats-top-k            Enumerate the most frequent values in a GeoMesa feature type
        version                Display the installed GeoMesa version


This usage text lists the available commands. To see help for an individual command run `geomesa help <command-name>` which for example
will give you something like this:

    $ geomesa help list
    List GeoMesa features for a given catalog
    Usage: list [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

The Accumulo username and password is required for each command. Specify the username and password
in each command by using `-u` or `--username `and `-p` or `--password`, respectively. One can also only specify the username
on the command line using `-u` or `--username` and type the password in an additional prompt, where the password will be
hidden from the shell history.

In all commands below, one can add `--instance-name`, `--zookeepers`, `--auths`, and `--visibilities` (or in short form `-i, -z`) arguments
to properly configure the Accumulo data store connector. The Accumulo instance name and Zookeepers string can usually
be automatically assigned as long as Accumulo is configured correctly. The Auths and Visibilities strings will have to
be added as arguments to each command, if needed.

### Installing SFT and Converter Definitions

Starting with version 1.2.3 GeoMesa Tools ships with embedded SimpleFeatureType and GeoMesa Converter definitions for common data types including Twitter, GeoNames, T-drive, and many more. Users can add additional types by providing a `reference.conf` file embedded with a jar within the `lib/common` directory or by registering the `reference.conf` file in the `$GEOMESA_HOME/conf/sfts` directory. 

For example, to add a type named `customtype`, create a directory named `$GEOMESA_HOME/conf/sfts/customtype` and then add the SFT and Conveter typesafe config to the a file named `$GEOMESA_HOME/conf/sfts/customtype/reference.conf`. This file will be automatically picked up and placed on the classpath when the tools are run.

### Downloading Common Datasets

GeoMesa ships with a script that will download common datasets and place them in a `$GEOMESA_HOME/data/<datatype>` directory. For example, to download gdelt data run:

    > $GEOMESA_HOME/bin/download-data.sh gdelt
    Enter a date in the form YYYYMMDD: 20150101
    Saving to: “/tmp/geomesa-tools-1.2.3-SNAPSHOT/data/gdelt/20150101.export.CSV.zip”
    
    > unzip -P data/gdelt/ data/gdelt/20150101.export.CSV.zip 
    Archive:  data/gdelt/20150101.export.CSV.zip
      inflating: 20150101.export.CSV     

    > geomesa ingest -u root -p <password> -c catalog -s gdelt -C gdelt $GEOMESA_HOME/data/gdelt/20150101.export.CSV

###Enabling Shape File Support
Due to licensing restrictions, a necessary dependency (jai-core) for shape file support must be manually installed:
    
    <dependency>
      <groupId>javax.media</groupId>
      <artifactId>jai_core</artifactId>
      <version>1.1.3</version>
    </dependency>
    
This library can be downloaded from your local nexus repo or `http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib.zip`

To install, copy the jai_core.jar and jai_codec.jar into `$GEOMESA_HOME/lib/`

Optionally there is a script bundled as `$GEOMESA_HOME/bin/install-jai.sh` that will attempt to wget and install
the jai libraries.


### Enabling Raster Ingest
Due to licensing restrictions, a number of necessary dependencies required for raster ingest must be manually installed:

    <dependency>
    	<groupId>org.jaitools</groupId>
    	<artifactId>jt-utils</artifactId>
    	<version>1.3.1</version>
    </dependency>
    <dependency>
        <groupId>javax.media</groupId>
        <artifactId>jai_core</artifactId>
        <version>1.1.3</version>
    </dependency>
    <dependency>
        <groupId>javax.media</groupId>
        <artifactId>jai_codec</artifactId>
        <version>1.1.3</version>
    </dependency>
    <dependency>
        <groupId>javax.media</groupId>
        <artifactId>jai_imageio</artifactId>
        <version>1.1</version>
    </dependency>


To install, you can either locate the JAR files or run the following included script which will attempt to wget and install the JARs.

`$GEOMESA_HOME/bin/install-jai.sh`
 
###Logging configuration
GeoMesa tools comes bundled by default with an slf4j implementation that is installed to the $GEOMESA_HOME/lib directory
 named `slf4j-log4j12-1.7.5.jar` If you already have an slf4j implementation installed on your Java Classpath you may
 see errors at runtime and will have to exclude one of the JARs. This can be done by simply renaming the bundled
 `slf4j-log4j12-1.7.5.jar` file to `slf4j-log4j12-1.7.5.jar.exclude`
 
Note that if no slf4j implementation is installed you will see this error:

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

## Command Explanations and Usage

### add-index
Use the `add-index` command to add a new index to an existing schema, or to update an index to a newer version.

#### Usage (required options denoted with star):
    $ geomesa help add-index
      Add or update indices for an existing GeoMesa feature type
      Usage: add-index [options]
        Options:
          --auths
             Accumulo authorizations
        * -c, --catalog
             Catalog table name for GeoMesa
          -q, --cql
             CQL predicate
        * -f, --feature-name
             Simple Feature Type name on which to operate
        * --index
             Name of index(es) to add - comma-separate or use multiple flags
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          --no-back-fill
             Do not copy any existing data into the new index
          -p, --password
             Accumulo password (will prompt if not supplied)
        * -u, --user
             Accumulo user name
          --visibilities
             Default feature visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)

#### Example command:
    geomesa add-index -u username -p password -c catalog -i instance -z zoo1,zoo2,zoo3 -f twitter --index z2


### create
To create a new feature on a specified catalog table, use the `create` command.  

#### Usage (required options denoted with star):
    $ geomesa help create
      Create a GeoMesa feature type
      Usage: create [options]
        Options:
          --auths
             Accumulo authorizations
        * -c, --catalog
             Catalog table name for GeoMesa
          --dtg
             DateTime field name to use as the default dtg
        * -f, --feature-name
             Simple Feature Type name on which to operate
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          -p, --password
             Accumulo password (will prompt if not supplied)
          -s, --spec
             SimpleFeatureType specification as a GeoTools spec string, SFT config, or
             file with either
          --use-shared-tables
             Use shared tables in Accumulo for feature storage (true/false)
             Default: true
        * -u, --user
             Accumulo user name
          --visibilities
             Accumulo scan visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa create -u username -p password -c test_create -i instance -z zoo1,zoo2,zoo3 -f testing -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 --dtg dtg

### remove-schema
To remove a feature type and it's associated data from a catalog table, use the `remove-schema` command.

#### Usage (required options denoted with star):
    $ geomesa help remove-schema
    Remove a schema and associated features from a GeoMesa catalog
    Usage: remove-schema [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -f, --feature-name
           Simple Feature Type name on which to operate
        --force
           Force deletion without prompt
           Default: false
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        --pattern
           Regular expression to select items to delete
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


####Example:
    geomesa remove-schema -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog -f testfeature1
    geomesa remove-schema -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog --pattern 'testfeatures\d+'


### stats-analyze
Use the `stats-analyze` command to update the statistics associated with your data. This can improve query planning.

#### Usage (required options denoted with star):
    $ geomesa help stats-analyze
    Analyze statistics on a GeoMesa feature type
    Usage: stats-analyze [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example:
    $ geomesa stats-analyze -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter
    Running stat analysis for feature type twitter...
    Stats analyzed:
      Total features: 8852601
      Bounds for geom: [ -171.75, -45.5903996, 157.7302, 89.99997102 ] cardinality: 2119237
      Bounds for dtg: [ '2016-02-01T00:09:12.000Z' to '2016-03-01T00:21:02.000Z' ] cardinality: 2161132
      Bounds for user_id: [ '100000215' to '99999502' ] cardinality: 861283
    Use 'stats-histogram' or 'stats-count' commands for more details

### stats-bounds
Use the `stats-bounds` command to view the bounds of your data for different attributes.

#### Usage
    $ geomesa help stats-bounds
    View or calculate bounds on attributes in a GeoMesa feature type
    Usage: stats-bounds [options]
      Options:
        -a, --attributes
           Attributes to evaluate (use multiple flags or separate with commas)
           Default: []
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -q, --cql
           CQL predicate
        --no-cache
           Calculate against the data set instead of using cached statistics (may be slow)
           Default: false
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example:
    $ geomesa stats-bounds -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter
      user_id [ 100000215 to 99999502 ] cardinality: 861283
      user_name [ unavailable ]
      text [ unavailable ]
      dtg [ 2016-02-01T00:09:12.000Z to 2016-03-01T00:21:02.000Z ] cardinality: 2161132
      geom [ -171.75, -45.5903996, 157.7302, 89.99997102 ] cardinality: 2119237

    $ geomesa stats-bounds -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter \
        --exact -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Running stat query...
        user_id [ 1011811424 to 99124417 ] cardinality: 115
        user_name [ bar_user to foo_user ] cardinality: 113
        text [ bar to foo ] cardinality: 180
        dtg [ 2016-02-02T00:01:07.000Z to 2016-02-02T23:59:41.000Z ] cardinality: 178
        geom [ -69.87212338, 45.01259299, -60.08925, 53.8868369 ] cardinality: 155

### stats-count
Use the `stats-count` command to count features in your data set.

#### Usage
    $ geomesa help stats-count
      Estimate or calculate feature counts in a GeoMesa feature type
      Usage: stats-count [options]
        Options:
          --auths
             Accumulo authorizations
        * -c, --catalog
             Catalog table name for GeoMesa
          -q, --cql
             CQL predicate
          --no-cache
             Calculate against the data set instead of using cached statistics (may be slow)
             Default: false
        * -f, --feature-name
             Simple Feature Type name on which to operate
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          -p, --password
             Accumulo password (will prompt if not supplied)
        * -u, --user
             Accumulo user name
          --visibilities
             Accumulo scan visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)

#### Example:
    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter
      Estimated count: 8852601

    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter \
        -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Estimated count: 2681

    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data -f twitter \
        --exact -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Running stat query...
      Count: 182

### stats-top-k

#### Usage
    $ geomesa help stats-top-k
      Enumerate the most frequent values in a GeoMesa feature type
      Usage: stats-top-k [options]
        Options:
          -a, --attributes
             Attributes to evaluate (use multiple flags or separate with commas)
             Default: []
          --auths
             Accumulo authorizations
        * -c, --catalog
             Catalog table name for GeoMesa
          -k
             Number of top values to show
          -q, --cql
             CQL predicate
        * -f, --feature-name
             Simple Feature Type name on which to operate
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          -p, --password
             Accumulo password (will prompt if not supplied)
        * -u, --user
             Accumulo user name
          --visibilities
             Accumulo scan visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)

#### Example:
    $ geomesa stats-top-k -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data \
        -f twitter -a user_id --no-cache
      Running stat query...
      Top values for 'user_id':
        3144822634 (26383)
        388009236 (20457)
        497145453 (19514)
        563319506 (15848)
        2841269945 (15716)
        ...

### stats-histogram
Use the `stats-histogram` command to view the values of different attributes, grouped into sorted bins.
If you query a histogram for a geometry attribute, the result will be displayed in an ASCII heatmap.

#### Usage
    $ geomesa help stats-histogram
      View or calculate counts of attribute in a GeoMesa feature type, grouped by sorted values
      Usage: stats-histogram [options]
        Options:
          -a, --attributes
             Attributes to evaluate (use multiple flags or separate with commas)
             Default: []
          --auths
             Accumulo authorizations
          -b, --bins
             How many bins the data will be divided into. For example, if you are
             examining a week of data, you may want to divide the date into 7 bins, one per day.
        * -c, --catalog
             Catalog table name for GeoMesa
          -q, --cql
             CQL predicate
          --no-cache
             Calculate against the data set instead of using cached statistics (may be slow)
             Default: false
        * -f, --feature-name
             Simple Feature Type name on which to operate
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          -p, --password
             Accumulo password (will prompt if not supplied)
        * -u, --user
             Accumulo user name
          --visibilities
             Accumulo scan visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)

#### Example:
    $ geomesa stats-histogram -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data \
        -f twitter -a dtg --bins 10
      Binned histogram for 'dtg':
        [ 2016-02-01T00:09:12.000Z to 2016-02-03T21:46:23.000Z ] 798968
        [ 2016-02-03T21:46:23.000Z to 2016-02-06T19:23:34.000Z ] 868019
        [ 2016-02-06T19:23:34.000Z to 2016-02-09T17:00:45.000Z ] 861720
        [ 2016-02-09T17:00:45.000Z to 2016-02-12T14:37:56.000Z ] 833473
        [ 2016-02-12T14:37:56.000Z to 2016-02-15T12:15:07.000Z ] 990292
        [ 2016-02-15T12:15:07.000Z to 2016-02-18T09:52:18.000Z ] 842434
        [ 2016-02-18T09:52:18.000Z to 2016-02-21T07:29:29.000Z ] 968936
        [ 2016-02-21T07:29:29.000Z to 2016-02-24T05:06:40.000Z ] 862808
        [ 2016-02-24T05:06:40.000Z to 2016-02-27T02:43:51.000Z ] 869208
        [ 2016-02-27T02:43:51.000Z to 2016-03-01T00:21:02.000Z ] 956743

    $ geomesa stats-histogram -u username -p password -i instance -z zoo1,zoo2,zoo3 -c geomesa.data \
        -f twitter -a dtg --bins 10 --exact
      Running stat query...
      Binned histogram for 'dtg':
        [ 2016-02-01T00:09:12.000Z to 2016-02-03T21:46:23.000Z ] 805620
        [ 2016-02-03T21:46:23.000Z to 2016-02-06T19:23:34.000Z ] 869361
        [ 2016-02-06T19:23:34.000Z to 2016-02-09T17:00:45.000Z ] 859868
        [ 2016-02-09T17:00:45.000Z to 2016-02-12T14:37:56.000Z ] 832458
        [ 2016-02-12T14:37:56.000Z to 2016-02-15T12:15:07.000Z ] 986829
        [ 2016-02-15T12:15:07.000Z to 2016-02-18T09:52:18.000Z ] 841580
        [ 2016-02-18T09:52:18.000Z to 2016-02-21T07:29:29.000Z ] 970460
        [ 2016-02-21T07:29:29.000Z to 2016-02-24T05:06:40.000Z ] 863484
        [ 2016-02-24T05:06:40.000Z to 2016-02-27T02:43:51.000Z ] 871742
        [ 2016-02-27T02:43:51.000Z to 2016-03-01T00:21:02.000Z ] 951199

### delete-raster
To delete a specific raster table use the `delete-raster` command.

#### Usage (required options denoted with star):
    $ geomesa help delete-raster
      Delete a GeoMesa Raster table
      Usage: delete-raster [options]
        Options:
          --auths
             Accumulo authorizations
          -f, --force
             Force deletion of feature without prompt
             Default: false
          -i, --instance
             Accumulo instance name
          --mock
             Run everything with a mock accumulo instance instead of a real one
             Default: false
          -p, --password
             Accumulo password (will prompt if not supplied)
        * -t, --raster-table
             Accumulo table for storing raster data
        * -u, --user
             Accumulo user name
          --visibilities
             Accumulo scan visibilities
          -z, --zookeepers
             Zookeepers (host[:port], comma separated)


####Example:
    geomesa delete-raster -u username -p password -t somerastertable -f
    

### delete-catalog
To delete a GeoMesa catalog completely (and all features in it) use the `delete-catalog` command.

#### Usage (required options denoted with star):
    $ geomesa help delete-catalog
    Delete a GeoMesa catalog completely (and all features in it)
    Usage: delete-catalog [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        --force
           Force deletion without prompt
           Default: false
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

    
####Example:
    geomesa delete-catalog -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog
    
### delete-features
To delete features from a table in GeoMesa. Does not delete any tables or schema information.

#### Usage (required options denoted with star):

    $ geomesa help delete-features
    Delete features from a table in GeoMesa. Does not delete any tables or schema information.
    Usage: delete-features [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -q, --cql
           CQL predicate
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)
    
### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  

#### Usage (required options denoted with star):
    $ geomesa help describe
    Describe the attributes of a given GeoMesa feature type
    Usage: describe [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

      
#### Example command:
    geomesa describe -u username -p password -c test_delete -f testing

### env
Prints out simple feature types and simple feature converters that are available on the current classpath.
Available types can be used for ingestion - see the `ingest` command.

#### Usage (required options denoted with star):
    $ geomesa help env
    Examine the current GeoMesa environment
    Usage: env [options]
      Options:
        --concise
           Render in concise format
           Default: false
        -c, --converters
           Describe specific GeoMesa converters
        --describe-converters
           Describe all the Simple Feature Type Converters
           Default: false
        --describe-sfts
           Describe all the Simple Feature Types
           Default: false
        --exclude-user-data
           Exclude user data
           Default: false
        --format
           Formats for sft (comma separated string, allowed values are typesafe,
           spec)
           Default: typesafe
        --list-converters
           List all the Converter Names
           Default: false
        --list-sfts
           List all the Simple Feature Types
           Default: false
        -s, --sfts
           Describe specific simple feature types
  

#### Example command:
    geomesa env --list-sfts --concise

### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.

#### Usage (required options denoted with star):
    $ geomesa help explain
    Explain how a GeoMesa query will be executed
    Usage: explain [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -q, --cql
           CQL predicate
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa explain -u username -p password -c test_catalog -f test_feature -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  

#### Usage (required options denoted with star):
    $ geomesa help export
    Export features from a GeoMesa data store
    Usage: export [options]
      Options:
        -a, --attributes
           Attributes from feature to export (comma-separated)...Comma-separated
           expressions with each in the format attribute[=filter_function_expression]|derived-attribute=filter_function_expression.
           filter_function_expression is an expression of filter function applied to attributes, literals and
           filter functions, i.e. can be nested
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -q, --cql
           CQL predicate
        --dt-attribute
           name of the date attribute to export
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -F, --format
           Format to export (csv|tsv|gml|json|shp|bin)
           Default: csv
        --gzip
           level of gzip compression to apply to output, from 1-9
        --id-attribute
           name of the id attribute to export
        -i, --instance
           Accumulo instance name
        --label-attribute
           name of the attribute to use as a bin file label
        --lat-attribute
           name of the latitude attribute to export
        --lon-attribute
           name of the longitude attribute to export
        -m, --max-features
           Maximum number of features to return. default: Long.MaxValue
           Default: 2147483647
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        --no-header
           Export as a delimited text format (csv|tsv) without a type header     
           Default: false
        -o, --output
           name of the file to output to instead of std out
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities      
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


<b>attribute expressions</b>
Attribute expressions are comma-separated expressions with each in the format 
    
    attribute[=filter_function_expression]|derived-attribute=filter_function_expression. 
    
`filter_function_expression` is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested

#### Example commands:
    geomesa export -u username -p password -c test_catalog -f test_feature -a "geom,text,user_name" --format csv -q "include" -m 100
    geomesa export -u username -p password -c test_catalog -f test_feature -a "geom,text,user_name" --format gml -q "user_name='JohnSmith'"
    geomesa export -u username -p password -c test_catalog -f test_feature -a "user_name,buf=buffer(geom\, 2)" \
        --format csv -q "[[ user_name like `John%' ] AND [ bbox(geom, 22.1371589, 44.386463, 40.228581, 52.379581, 'EPSG:4326') ]]"


### gen-avro-schema
Generate an Avro schema from a SimpleFeatureType

#### Usage (required options denoted with star):
    $ geomesa help gen-avro-schema
    Convert SimpleFeatureTypes to Avro schemas
    Usage: gen-avro-schema [options]
      Options:
        -f, --feature-name
           Simple Feature Type name on which to operate
      * -s, --spec
           SimpleFeatureType specification as a GeoTools spec string, SFT config, or
           file with either

#### Example commands:
    # Convert an SFT loaded from config
    geomesa gen-avro-schema -s example-csv

    # convert an SFT passed in as an argument
    geomesa gen-avro-schema -s "name:String,dtg:Date,geom:Point" -f myfeature


### get-sft-config
Gets an existing simple feature type as an encoded string.

#### Usage (required options denoted with star):
    $ geomesa help get-sft-config
    Get the SimpleFeatureType of a feature
    Usage: get-sft-config [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        --concise
           Render in concise format
           Default: false
        --exclude-user-data
           Exclude user data
           Default: false
      * -f, --feature-name
           Simple Feature Type name on which to operate
        --format
           Formats for sft (comma separated string, allowed values are typesafe,
           spec)
           Default: typesafe
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa get-sft-config -u username -p password -c test_catalog -f test_feature -i instance

### ingest
Ingests various file formats into GeoMesa using the GeoMesa Converter Framework. Converters are specified in HOCON 
format (https://github.com/typesafehub/config/blob/master/HOCON.md). GeoMesa defines several common converter factories 
for formats such as delimited text (TSV, CSV), fixed width files, JSON, XML, and Avro. New converter factories (e.g. 
for custom binary formats) can be registered on the classpath using Java SPI. Shapefile ingest is also supported. Files 
can be either local or in HDFS. You cannot mix target files (e.g. local and HDFS).

#### Usage (required options denoted with star):
    $ geomesa help ingest
    Ingest/convert various file formats into GeoMesa
    Usage: ingest [options] <file>...
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -C, --converter
           GeoMesa converter specification as a config string or name of an
           available converter
        -f, --feature-name
           Simple Feature Type name on which to operate
        -F, --format
           indicate non-converter ingest (shp)
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -s, --spec
           SimpleFeatureType specification as a GeoTools spec, SFT config, or name
           of an available type
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Defining Converters and SFTs

Converters and SFTs are specified in HOCON format (https://github.com/typesafehub/config/blob/master/HOCON.md) and 
loaded using TypeSafe config. They can be referenced by name using the ``-s`` and ``-C`` args.

To define new converters for the users can package a ``reference.conf`` file inside a JAR and drop it in the 
``$GEOMESA_HOME/lib`` directory or add config definitions to the ``$GEOMESA_TOOLS/conf/application.conf`` file which 
includes some examples. SFT and Converter specifications should use the path prefixes 
``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>``

##### Ingest with Converters and SFTs specified by name

For example...Here's a simple CSV file to ingest:

    $ cat example1.csv
    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

A SimpleFeatureType named ``renegades`` and a converter named ``renegades-csv`` can be placed in the application.conf:

    # cat $GEOMESA_HOME/conf/application.conf
    geomesa {
      sfts {
        renegades = {
          attributes = [
            { name = "id",       type = "Integer",      index = false                             }
            { name = "name",     type = "String",       index = true                              }
            { name = "age",      type = "Integer",      index = false                             }
            { name = "lastseen", type = "Date",         index = true                              }
            { name = "friends",  type = "List[String]", index = true                              }
            { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
          ]
        }
      }
      converters {
        renegades-csv = {
          type   = "delimited-text"
          format = "CSV"
          options {
            skip-lines = 1 // skip the header
          }
          id-field = "toString($id)"
          fields = [
            { name = "id",       transform = "$1::int"                 }
            { name = "name",     transform = "$2::string"              }
            { name = "age",      transform = "$3::int"                 }
            { name = "lastseen", transform = "date('YYYY-MM-dd', $4)"  }
            { name = "friends",  transform = "parseList('string', $5)" }
            { name = "lon",      transform = "$6::double"              }
            { name = "lat",      transform = "$7::double"              }
            { name = "geom",     transform = "point($lon, $lat)"       }
          ]
        }
      }
    }

The csv file can then be ingested by referencing the SFT and Converter by name:
  
    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s renegades -C renegades-csv hdfs:///some/hdfs/path/to/example.csv

##### Providing SFT and Converter configs as arguments

SFT and Converter configs can also be provided as strings or filenames to the ``-s`` and ``-C`` arguments. The syntax is
very similar to the ``application.conf`` and ``reference.conf`` format. Config specifications must be nested using the 
paths ``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>`` as shown below:

    # A nested SFT config provided as a string or file to the -s argument specifying
    # a type named "renegades"
    #
    # cat /tmp/renegades.sft
    geomesa.sfts.renegades = {
      attributes = [
        { name = "id",       type = "Integer",      index = false                             }
        { name = "name",     type = "String",       index = true                              }
        { name = "age",      type = "Integer",      index = false                             }
        { name = "lastseen", type = "Date",         index = true                              }
        { name = "friends",  type = "List[String]", index = true                              }
        { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
      ]
    }
   
Similarly, converter configurations must be nested when passing them directly to the ``-C`` argument:

    # a nested converter definition
    # cat /tmp/renegades.convert    
    geomesa.converters.renegades-csv = {
      type   = "delimited-text"
      format = "CSV"
      options {
        skip-lines = 0 // don't skip lines in distributed ingest
      }
      id-field = "toString($id)"
      fields = [
        { name = "id",       transform = "$1::int"                 }
        { name = "name",     transform = "$2::string"              }
        { name = "age",      transform = "$3::int"                 }
        { name = "lastseen", transform = "date('YYYY-MM-dd', $4)"  }
        { name = "friends",  transform = "parseList('string', $5)" }
        { name = "lon",      transform = "$6::double"              }
        { name = "lat",      transform = "$7::double"              }
        { name = "geom",     transform = "point($lon, $lat)"       }
      ]
    }    
    
Using the SFT and Converter config files we can then ingest our csv file with this command:

    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s /tmp/renegades.sft -C /tmp/renegades.convert hdfs:///some/hdfs/path/to/example.csv

##### More info on Converters

For more documentation on converter configuration, check out the [geomesa-convert README](http://github.com/locationtech/geomesa/geomesa-convert/README.md)

#### Ingest a shape file

    geomesa ingest -u username -p password -c test_catalog -f shapeFileFeatureName /some/path/to/file.shp

#### Enabling S3 Ingest

Hadoop ships with an implementation of a S3 filesystems that can be enabled in the Hadoop configuration used with GeoMesa Tools. GeoMesa Tools can perform ingest using both the second-generation (`s3n`) and third-generation (`s3a`) filesystems. Edit the `$HADOOP_CONF_DIR/core-site.xml` file in your Hadoop installation, as shown below. These instructions apply to Hadoop 2.5.0 and higher. Note that you must have the environment variable ``HADOOP_MAPRED_HOME`` set properly in your environment. Some configurations can substitute ``HADOOP_PREFIX`` in the classpath values below.

Warning: AWS credentials are valuable. They pay for services and control read and write protection for data. If you are running GeoMesa on AWS EC2 instances, it is recommended to use s3a. With s3a, you can omit the Access Key Id and Secret Access keys from `core-site.xml` and rely on IAM roles. 

##### s3a

    <!-- core-site.xml -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for mapreduce jobs. This override is needed so that s3 URLs work on hadoop 2.6.0+</description>
    </property>

    <!-- OMIT these keys if running on AWS EC2; use IAM roles instead -->
    <property>
        <name>fs.s3a.access.key</name>
        <value>XXXX YOURS HERE</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>XXXX YOURS HERE</value>
        <description>Valueable credential - do not commit to CM</description>
    </property>
 

After you have enabled S3 in your Hadoop configuration you can ingest with GeoMesa tools. Note that you can still use the Kleene star (*) with S3.

    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3a://bucket/path/file* 

##### s3n

    <!-- core-site.xml -->
    <!-- Note that you need to make sure HADOOP_MAPRED_HOME is set or some other way of getting this on the classpath -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for mapreduce jobs. This override is needed so that s3 URLs work on hadoop 2.6.0+</description>
    </property>
    <property>
        <name>fs.s3n.impl</name>
        <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
        <description>Tell hadoop which class to use to access s3 URLs. This change became necessary in hadoop 2.6.0</description>
    </property>
    <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>XXXX YOURS HERE</value>
    </property>

    <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>XXXX YOURS HERE</value>
    </property>

S3n paths are prefixed in hadoop with ``s3n://`` as shown below:

    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3n://bucket/path/file s3n://bucket/path/*


### ingest-raster
To ingest one or multiple raster image files into Geomesa, use the `ingest-raster` command. Input files, Geotiff or
DTED, are located on local file system. If chunking (only works for single file) is specified by option `-ck`,
input file is cut into chunks by size in kilobytes (option `-cs or --chunk-size`) and chunks are ingested. Ingestion
is done in local or distributed mode (by option `-m or --mode`, default is local). In local mode, files are ingested
directly from local host into Accumulo tables. In distributed mode, raster files are serialized and stored in a HDFS
directory from where they are ingested.

*Note:* Make sure GDAL is installed when doing chunking that depends on GDAL utility `gdal_translate`.

*Note:* It assumes input raster files have CRS set to EPSG:4326. For non-EPSG:4326 files, they need to be converted into
EPSG:4326 raster files before ingestion. An example of doing conversion with GDAL utility is `gdalwarp -t_srs EPSG:4326
input_file out_file`.

#### Usage (required options denoted with star):
    $ geomesa help ingest-raster
    Ingest raster files into GeoMesa
    Usage: ingest-raster [options]
      Options:
        --auths
           Accumulo authorizations
      * -f, --file
           Single raster file or directory of raster files to be ingested
        -F, --format
           Format of incoming raster data (geotiff | DTED) to override file
           extension recognition
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -P, --parallel-level
           Maximum number of local threads for ingesting multiple raster files
           (default to 1)
           Default: 1
        -p, --password
           Accumulo password (will prompt if not supplied)
        --query-threads
           Threads for querying raster data
      * -t, --raster-table
           Accumulo table for storing raster data
        -T, --timestamp
           Ingestion time (default to current time)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        --write-memory
           Memory allocation for ingestion operation
        --write-threads
           Threads for writing raster data
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa ingest-raster -u username -p password -t geomesa_raster -f /some/local/path/to/raster.tif


### add-attribute-index
Add an attribute index for a specified list of attributes. 

#### Usage (required options denoted with star):
    $ geomesa help add-attribute-index
    Run a Hadoop map reduce job to add an index for attributes
    Usage: add-attribute-index [options]
      Options:
      * -a, --attributes
           Attributes to evaluate (comma-separated)
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * --coverage
           Type of index (join or full)
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    $ geomesa add-attribute-index -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog \
        -f test_feature -a attribute1,attribute2 --coverage full


### keywords
To add, remove, or list all the keywords on a specified catalog table, use the `keywords` command

#### Usage (required options denoted with star):
    $ geomesa help keywords
    Add/Remove/List keywords on an existing schema
    Usage: keywords [options]
      Options:
        -a, --add
           A keyword to add. Can be specified multiple times
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -f, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -l, --list
           List all keywords on the schema
           Default: false
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -r, --remove
           A keyword to remove. Can be specified multiple times
        --removeAll
           Remove all keywords on the schema
           Default: false
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa keywords -u username -p password -c test_catalog -f feature_name -i instance \
    -a keywordA -a keywordB=foo,bar -r keywordC -l


### list
To list the features on a specified catalog table, use the `list` command.  

#### Usage (required options denoted with star):
    $ geomesa help list
    List GeoMesa feature types for a given catalog
    Usage: list [options]
      Options:
        --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa list -u username -p password -c test_catalog


### query-raster-stats
Export queries and statistics logged for raster tables by using the `querystats` command.

#### Usage (required options denoted with star):
    $ geomesa help query-raster-stats
    Export queries and statistics about the last X number of queries to a CSV file.
    Usage: query-raster-stats [options]
      Options:
        --auths
           Accumulo authorizations
        -i, --instance
           Accumulo instance name
        --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -n, --number-of-records
           Number of query records to export from Accumulo
           Default: 1000
        -o, --output
           Name of the file to output to
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -t, --raster-table
           Accumulo table for storing raster data
      * -u, --user
           Accumulo user name
        --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa query-raster-stats -u username -p password -t somerastertable -num 10
    
### config-table
To list, describe, and update the configuration parameters on a specified table, use the `config-table` command. 

#### Usage (required options denoted with star):
    $ geomesa help config-table
    Perform table configuration operations
    Usage: config-table [options] [command] [command options]
      Commands:
        list      List the configuration parameters for a geomesa table
          Usage: list [options]
            Options:
              --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -f, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        describe      Describe a given configuration parameter for a table
          Usage: describe [options]
            Options:
              --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -f, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            * -P, --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        update      Update a given table configuration parameter
          Usage: update [options]
            Options:
              --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -f, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            * -n, --new-value
                 New value of the property
            * -P, --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa config-table list -u username -p password -c test_catalog -f test_feature -t st_idx
    geomesa config-table describe -u username -p password -c test_catalog -f test_feature -t attr_idx --param table.bloom.enabled
    geomesa config-table update -u username -p password -c test_catalog -f test_feature -t records --param table.bloom.enabled -n true

### version
Prints out the version, git branch, and commit ID that the tools were built with.

#### Usage:
    $geomesa help version
    Display the installed GeoMesa version
    Usage: version

#### Example commands:
    geomesa version

## GeoMesa Kafka Command Line Tools
GeoMesa also comes with Kafka command line tools.  The tools use kafka 0.8 jars by default, so when using kafka 0.9
 you will need to make the following replacements in the lib/kafka directory:

    geomesa-kafka-datastore-<version>.jar   replace with     geomesa-kafka-09-datastore-<version>.jar
    kafka_2.11-0.8.2.1.jar                  replace with     kafka_2.11-0.9.0.1.jar
    kafka-clients-0.8.2.1.jar               replace with     kafka-clients-0.9.0.1.jar
    zkclient-0.3.jar                        replace with     zkclient-0.7.jar

You can run the tools with:

    geomesa-kafka
    
This should print out the following usage text: 
    
    $ geomesa-kafka
    Usage: geomesa-kafka [command] [command options]
      Commands:
        create          Create a feature definition in GeoMesa
        describe        Describe the attributes of a given feature in GeoMesa
        help            Show help
        list            List GeoMesa features for a given zkPath
        listen          Listen to a GeoMesa Kafka topic
        remove-schema    Remove a schema and associated features from GeoMesa
        version         GeoMesa Version

### create
Used to create a feature type `SimpleFeatureType` at the specified zkpath.

#### Usage:

    $ geomesa-kafka help create
    Create a feature definition in GeoMesa
    Usage: create [options]
      Options:
      * -b, --brokers
           Brokers (host:port, comma separated)
      * -f, --feature-name
           Simple Feature Type name on which to operate
        --partitions
           Number of partitions for the Kafka topic
        --replication
           Replication factor for Kafka topic
      * -s, --spec
           SimpleFeatureType specification as a GeoTools spec string, SFT config, or
           file with either
      * -p, --zkpath
           Zookeeper path where feature schemas are saved
      * -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example command:
    
    $ geomesa-kafka create -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      -p /geomesa/ds/kafka

### describe
Display details about the attributes of a specified feature type.

#### Usage:

    $ geomesa-kafka help describe
    Describe the attributes of a given feature in GeoMesa
    Usage: describe [options]
      Options:
      * -b, --brokers
           Brokers (host:port, comma separated)
      * -f, --feature-name
           Simple Feature Type name on which to operate
      * -p, --zkpath
           Zookeeper path where feature schemas are saved
      * -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example command:

    $ geomesa-kafka describe -f testfeature -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092 -p /geomesa/ds/kafka

### list
List all known feature types in Kafka. If no zkpath parameter is specified, the list command will search all of zookeeper for potential feature types.

#### Usage:

    $ geomesa-kafka help list
    List GeoMesa features for a given zkPath
    Usage: list [options]
      Options:
      * -b, --brokers
           Brokers (host:port, comma separated)
        -p, --zkpath
           Zookeeper path where feature schemas are saved
      * -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:

    $ geomesa-kafka list -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

### listen
Logs out the messages written to a topic corresponding to the passed in feature type.

#### Usage:

    $ geomesa-kafka help listen
    Listen to a GeoMesa Kafka topic
    Usage: listen [options]
      Options:
      * -b, --brokers
           Brokers (host:port, comma separated)
      * -f, --feature-name
           Simple Feature Type name on which to operate
        --from-beginning
           Consume from the beginning or end of the topic
           Default: false
      * -p, --zkpath
           Zookeeper path where feature schemas are saved
      * -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example command:

    $ geomesa-kafka listen -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka \
      --from-beginning

### remove-schema
Used to remove a feature type `SimpleFeatureType` in a GeoMesa catalog. This will also delete any feature of that type in the data store.

#### Usage:

    $ geomesa-kafka help remove-schema
    Remove a schema and associated features from GeoMesa
    Usage: remove-schema [options]
      Options:
      * -b, --brokers
           Brokers (host:port, comma separated)
        -f, --feature-name
           Simple Feature Type name on which to operate
        --force
           Force deletion without prompt
           Default: false
        --pattern
           Regular expression to select items to delete
      * -p, --zkpath
           Zookeeper path where feature schemas are saved
      * -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:

    $ geomesa-kafka remove-schema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka
    $ geomesa-kafka remove-schema --pattern 'testfeature\d+' \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka

### version
Prints out the version, git branch, and commit ID that the tools were built with.

#### Usage: 
   
    $ geomesa-kafka help version
    GeoMesa Version
    Usage: version [options]
    
#### Example command:

    $ geomesa-kafka version
