# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
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
        create              Create a feature definition in a GeoMesa catalog
        deletecatalog       Delete a GeoMesa catalog completely (and all features in it)
        deleteraster        Delete a GeoMesa Raster Table
        describe            Describe the attributes of a given feature in GeoMesa
        env                 Examine the current GeoMesa environment
        explain             Explain how a GeoMesa query will be executed
        export              Export a GeoMesa feature
        getsft              Get the SimpleFeatureType of a feature
        help                Show help
        ingest              Ingest/convert various file formats into GeoMesa
        ingestraster        Ingest a raster file or raster files in a directory into GeoMesa
        list                List GeoMesa features for a given catalog
        queryrasterstats    Export queries and statistics about the last X number of queries to a CSV file.
        removeschema        Remove a schema and associated features from a GeoMesa catalog
        genavroschema       Convert SimpleFeatureTypes to Avro schemas
        tableconf           Perform table configuration operations
        version             GeoMesa Version



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

###Enabling Shape File Support
Due to licensing restrictions, a necessary dependency (jai-core) for shape file support must be manually installed:
    
    <dependency>
      <groupId>javax.media</groupId>
      <artifactId>jai_core</artifactId>
      <version>1.1.3</version>
    </dependency>
    
This library can be downloaded from your local nexus repo or `http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib.zip`

To install, copy the jai_core.jar and jai_codec.jar into `$GEOMESA_HOME/lib/`

Optionally there is a script bundled as `$GEOMESA_HOME/bin/install-jai` that will attempt to wget and install 
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

`$GEOMESA_HOME/bin/install-jai`
 
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

### create
To create a new feature on a specified catalog table, use the `create` command.  

#### Usage (required options denoted with star):
    $ geomesa help create
      Create a feature definition in a GeoMesa catalog
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

### removeschema
To remove a feature type and it's associated data from a catalog table, use the `removeschema` command.

#### Usage (required options denoted with star):
    $ geomesa help removeschema
    Remove a schema and associated features from a GeoMesa catalog
    Usage: removeschema [options]
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
    geomesa removeschema -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog -f testfeature1
    geomesa removeschema -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog --pattern 'testfeatures\d+'
    
    
### deleteraster
To delete a specific raster table use the `deleteraster` command.

#### Usage (required options denoted with star):
    $ geomesa help deleteraster
      Delete a GeoMesa Raster Table
      Usage: deleteraster [options]
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
    geomesa deleteraster -u username -p password -t somerastertable -f
    

### deletecatalog
To delete a GeoMesa catalog completely (and all features in it) use the `deletecatalog` command.

#### Usage (required options denoted with star):
    $ geomesa help deletecatalog
    Delete a GeoMesa catalog completely (and all features in it)
    Usage: deletecatalog [options]
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
    geomesa deletecatalog -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog
    
### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  

#### Usage (required options denoted with star):
    $ geomesa help describe
    Describe the attributes of a given feature in GeoMesa
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
        -c, --converters
           Examine GeoMesa converters
        -s, --sfts
           Examine simple feature types

#### Example command:
    geomesa env

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
    Export a GeoMesa feature
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

### getsft
Gets an existing simple feature type as an encoded string.

#### Usage (required options denoted with star):
    $ geomesa help getsft
    Get the SimpleFeatureType of a feature
    Usage: getsft [options]
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
    geomesa getsft -u username -p password -c test_catalog -f test_feature -i instance

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

Warning: AWS credentials are valueable. They pay for services and control read and write protection for data. If you are running GeoMesa on AWS EC2 instances, it is recommended to use s3a. With s3a, you can omit the Access Key Id and Secret Access keys from `core-site.xml` and rely on IAM roles. 

##### s3a

    <!-- core-site.xml -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for mapreduce jobs. This override is neeeded so that s3 URLs work on hadoop 2.6.0+</description>
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


### ingestraster
To ingest one or multiple raster image files into Geomesa, use the `ingestraster` command. Input files, Geotiff or
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
    $ geomesa help ingestraster
    Ingest a raster file or raster files in a directory into GeoMesa
    Usage: ingestraster [options]
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
           Threads for quering raster data
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
    geomesa ingestraster -u username -p password -t geomesa_raster -f /some/local/path/to/raster.tif

### list
To list the features on a specified catalog table, use the `list` command.  

#### Usage (required options denoted with star):
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


#### Example command:
    geomesa list -u username -p password -c test_catalog
    
### querystats
Export queries and statistics logged for raster tables by using the `querystats` command.

#### Usage (required options denoted with star):
    $ geomesa help queryrasterstats
    Export queries and statistics about the last X number of queries to a CSV file.
    Usage: querystats [options]
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
    geomesa queryrasterstats -u username -p password -t somerastertable -num 10

### genavroschema 
Generate an Avro schema from a SimpleFeatureType

#### Usage (required options denoted with star):
    $ geomesa help genavroschema
    Convert SimpleFeatureTypes to Avro schemas
    Usage: genavroschema [options]
      Options:
        -f, --feature-name
           Simple Feature Type name on which to operate
      * -s, --spec
           SimpleFeatureType specification as a GeoTools spec string, SFT config, or
           file with either

#### Example commands:
    # Convert an SFT loaded from config
    geomesa genavroschema -s example-csv
    
    # convert an SFT passed in as an argument
    geomesa genavroschema -s "name:String,dtg:Date,geom:Point" -f myfeature
    
### tableconf
To list, describe, and update the configuration parameters on a specified table, use the `tableconf` command. 

#### Usage (required options denoted with star):
    $ geomesa help tableconf
    Perform table configuration operations
    Usage: tableconf [options] [command] [command options]
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
    geomesa tableconf list -u username -p password -c test_catalog -f test_feature -t st_idx
    geomesa tableconf describe -u username -p password -c test_catalog -f test_feature -t attr_idx --param table.bloom.enabled
    geomesa tableconf update -u username -p password -c test_catalog -f test_feature -t records --param table.bloom.enabled -n true

### version
Prints out the version, git branch, and commit ID that the tools were built with.

#### Usage:
    $geomesa help version
    GeoMesa Version
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
        removeschema    Remove a schema and associated features from GeoMesa
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

### removeschema
Used to remove a feature type `SimpleFeatureType` in a GeoMesa catalog. This will also delete any feature of that type in the data store.

#### Usage:

    $ geomesa-kafka help removeschema
    Remove a schema and associated features from GeoMesa
    Usage: removeschema [options]
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

    $ geomesa-kafka removeschema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka
    $ geomesa-kafka removeschema --pattern 'testfeature\d+' \
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
