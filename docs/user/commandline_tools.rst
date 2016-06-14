Command Line Tools
==================

This chapter describes GeoMesa Tools, a set of command line tools for feature
management, query planning and explanation, ingest, and export from
the command line.

Installation
------------

See :ref:`setting_up_commandline`.

Running the command line tools
------------------------------

Run ``geomesa`` without any arguments to produce the following usage text::

    $ geomesa
    Warning: GEOMESA_HOME is not set, using /opt/devel/src/geomesa/geomesa-dist/target/geomesa-1.2.0-SNAPSHOT/dist/tools/geomesa-tools-1.2.0-SNAPSHOT
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
        keywords            Add/remove keywords on an existing schema
        list                List GeoMesa features for a given catalog
        queryrasterstats    Export queries and statistics about the last X number of queries to a CSV file.
        removeschema        Remove a schema and associated features from a GeoMesa catalog
        tableconf           Perform table configuration operations
        version             GeoMesa Version

This usage text lists the available commands. To see help for an individual command,
run ``geomesa help <command-name>``, which for example will give you something like this::

    $ geomesa help list
    Warning: GEOMESA_HOME is not set, using /opt/devel/src/geomesa/geomesa-dist/target/geomesa-1.2.0-SNAPSHOT/dist/tools/geomesa-tools-1.2.0-SNAPSHOT
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

The Accumulo username and password is required for each command. Specify the
username and password in each command by using ``-u`` (or ``--username``) and ``-p`` (or
``--password``), respectively. One can also only specify the username on the
command line using ``-u`` or ``--username`` and type the password in an additional
prompt, where the password will be hidden from the shell history.

In nearly all commands below, one can add ``--instance-name``, ``--zookeepers``,
``--auths``, and ``--visibilities`` (or in short form ``-i``, ``-z``, ``-a``, and ``-v``) arguments
to properly configure the Accumulo data store connector. The Accumulo instance
name and Zookeepers string can usually be automatically assigned as long as
Accumulo is configured correctly. The Auths and Visibilities strings will have
to be added as arguments to each command, if needed.

Command overview
----------------
Creating and deleting feature types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

create
~~~~~~

Used to create a feature type (``SimpleFeatureType``)  in a GeoMesa catalog::

    $ geomesa create -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 \
      -c test_create \
      -f testing \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      --dtg dtg


describe
~~~~~~~~

Display details about the attributes of a specified feature type::

    $ geomesa describe -u username -p password -c test_delete -f testing

getsft
~~~~~~

Get the specified feature type as a typesafe config::

    $ geomesa getsft -u username -p password -c test_catalog -f test_feature --format typesafe

Get the specified feature type as an encoded feature schema string::

    $ geomesa getsft -u username -p password -c test_catalog -f test_feature --format spec

keywords
~~~~~~~~

Add or remove keywords to a specified schema::
Repeat the -a or -r flags to add or remove multiple keywords
The ``--removeAll``` option removes all keywords
The ```-l``` option lists the schema's keywords following all operations
If there is whitespace within a keyword, enclose it in quotes for proper functionality

    $ geomesa keywords -u username -p password \
        -a keywordB -a keywordC -r keywordA -l\
        -i instance -z zoo1,zoo2,zoo3 \
        -c catalog -f featureTypeName

list
~~~~

List all known feature types in a GeoMesa catalog::

    $ geomesa list -u username -p password -c test_catalog

removeschema
~~~~~~~~~~~~

Used to remove a feature type (``SimpleFeatureType``) in a GeoMesa catalog. This will also delete any feature of that type in the data store::

    $ geomesa removeschema -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 \
      -c test_catalog -f testfeature1
    $ geomesa removeschema -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 \
      -c test_catalog --pattern 'testfeatures\d+'

Ingesting and exporting data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _export:

export
~~~~~~

Export GeoMesa features. The "attribute expressions" specified by the ``-a`` option are comma-separated expressions 
in the format::
    
    attribute[=filter_function_expression]|derived-attribute=filter_function_expression
    
`filter_function_expression` is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested.

Example export commands::

    $ geomesa export -u username -p password \
      -c test_catalog -f test_feature \
      -a "geom,text,user_name" --format csv \
      -q "include" -m 100
    $ geomesa export -u username -p password \
       -c test_catalog -f test_feature \
       -a "geom,text,user_name" --format gml \
       -q "user_name='JohnSmith'"
    $ geomesa export -u username -p password \
      -c test_catalog -f test_feature \
      -a "user_name,buf=buffer(geom\, 2)" \
       --format csv -q "[[ user_name like `John%' ] AND [ bbox(geom, 22.1371589, 44.386463, 40.228581, 52.379581, 'EPSG:4326') ]]"

.. _ingest:

ingest
~~~~~~

Used to convert and ingest data from various file formats as GeoMesa features.

GeoMesa defines several common converter factories for formats such as delimited text
(TSV, CSV), fixed width files, JSON, XML, and Avro. New converter factories (e.g. for custom binary formats) can be
registered on the classpath using Java SPI. Shapefile ingest is also supported. Files can be either local or in HDFS.
You cannot mix target files (e.g. local and HDFS).

.. note::

    The header, if present, is not parsed by ``ingest`` for information. It is assumed that all lines are valid entries.

Converters and SFTs are specified in HOCON format (https://github.com/typesafehub/config/blob/master/HOCON.md) and
loaded using TypeSafe config. They can be referenced by name using the ``-s`` and ``-C`` args.

To define new converters for the users can package a ``reference.conf`` file inside a jar and drop it in the
``$GEOMESA_HOME/lib`` directory or add config definitions to the ``$GEOMESA_TOOLS/conf/application.conf`` file which
includes some examples. SFT and Converter specifications should use the path prefixes
``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>``

For example...Here's a simple CSV file to ingest named ``example.csv``::

    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

To ingest this file, a SimpleFeatureType named ``renegades`` and a converter named ``renegades-csv`` can be placed in
the application.conf file::

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
            skip-lines = 1 //skip the header
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


The SFT and Converter can be referenced by name and the following commands can ingest the file::

    $ geomesa ingest -u username -p password \
      -c geomesa_catalog -i instance \
      -s renegates -C renegades-csv example1.csv
    # use the Hadoop file system instead
    $ geomesa ingest -u username -p password \
      -c geomesa_catalog -i instance \
      -s renegades -C renegades-csv hdfs:///some/hdfs/path/to/example1.csv

SFT and Converter configs can also be provided as strings or filenames to the ``-s`` and ``-C`` arguments. The syntax is
very similar to the ``application.conf`` and ``reference.conf`` format. Config specifications must be nested using the
paths ``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>`` as shown below::

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

Similarly, converter configurations must be nested when passing them directly to the ``-C`` argument::

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

Using the SFT and Converter config files we can then ingest our csv file with this command::

    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s /tmp/renegades.sft -C /tmp/renegades.convert hdfs:///some/hdfs/path/to/example.csv


For more documentation on converter configuration, refer to the the ``geomesa-$VERSION/docs/README-convert.md`` file
in the binary distribution.

Shape files may also be ingested::

    $ geomesa ingest -u username -p password -c test_catalog -f shapeFileFeatureName /some/path/to/file.shp

**Enabling S3 Ingest**

Hadoop ships with an implementation of a S3 filesystems that can be enabled in the Hadoop configuration used with GeoMesa Tools. GeoMesa Tools can perform ingest using both the second-generation (`s3n`) and third-generation (`s3a`) filesystems. Edit the `$HADOOP_CONF_DIR/core-site.xml` file in your Hadoop installation, as shown below. These instructions apply to Hadoop 2.5.0 and higher. Note that you must have the environment variable ``HADOOP_MAPRED_HOME`` set properly in your environment. Some configurations can substitute ``HADOOP_PREFIX`` in the classpath values below.

.. note:: 

    Warning: AWS credentials are valueable. They pay for services and control read and write protection for data. If you are running GeoMesa on AWS EC2 instances, it is recommended to use s3a. With s3a, you can omit the Access Key Id and Secret Access keys from `core-site.xml` and rely on IAM roles.:

s3a::

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
 


After you have enabled S3 in your Hadoop configuration you can ingest with GeoMesa tools. Note that you can still use the Kleene star (*) with S3.:

    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3a://bucket/path/file* 

s3n::

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

S3n paths are prefixed in hadoop with ``s3n://`` as shown below::

    geomesa ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3n://bucket/path/file s3n://bucket/path/*



Working with raster data
^^^^^^^^^^^^^^^^^^^^^^^^

deleteraster
~~~~~~~~~~~~

Delete a given GeoMesa raster table::

    $ geomesa deleteraster -u username -p password -t somerastertable -f

ingestraster
~~~~~~~~~~~~

Ingest one or multiple raster image files into Geomesa. Input files, GeoTIFF or
DTED, should be located on the local file system. 

.. note:: 

    Make sure GDAL is installed when doing chunking, which depends on the GDAL utility ``gdal_translate``.

    Input raster files are assumed to have CRS set to EPSG:4326. For non-EPSG:4326 files, they need to be converted into
    EPSG:4326 raster files before ingestion. An example of doing conversion with GDAL utility is ``gdalwarp -t_srs EPSG:4326
    input_file out_file``.

Example usage::

    $ geomesa ingestraster -u username -p password -t geomesa_raster -f /some/local/path/to/raster.tif

queryrasterstats
~~~~~~~~~~~~~~~~

Export queries and statistics about the `n` most recent raster queries to a CSV file::

    $ geomesa queryrasterstats -u username -p password -t somerastertable -n 10


Performing system administration tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

deletecatalog
~~~~~~~~~~~~~

Delete a GeoMesa catalog table completely, along with all features in it.::

    $ geomesa deletecatalog -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog
 
env
~~~

Examines the current GeoMesa tools environment, and prints out simple feature types converters that 
are available on the current classpath. The available types can be used for ingestion; see the :ref:`ingest` command.

Example usage::

    $ geomesa env

explain
~~~~~~~

Explain how a given GeoMesa query will be executed::

    $ geomesa explain -u username -p password \
      -c test_catalog -f test_feature \
      -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

tableconf
~~~~~~~~~

Perform various table configuration tasks. There are three sub-arguments:

 * **list** - List the configuration options for a GeoMesa table
 * **describe** - Describe a given configuration option for a table
 * **update** - Update a given configuration option for a table

Example commands::

    $ geomesa tableconf list -u username -p password \
      -c test_catalog -f test_feature -t st_idx
    $ geomesa tableconf describe -u username -p password \
      -c test_catalog -f test_feature -t attr_idx \
      --param table.bloom.enabled
    $ geomesa tableconf update -u username -p password \
      -c test_catalog -f test_feature -t records \
      --param table.bloom.enabled -n true

version
~~~~~~~

Prints out the version, git branch, and commit ID that the tools were built with::

    $ geomesa version


Kafka command line tools
------------------------

Run ``geomesa-kafka`` without any arguments to produce the following usage text::

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

This usage text lists the available commands. To see help for an individual command,
run ``geomesa-kafka help <command-name>``, which for example will give you something like this::

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

Command overview
^^^^^^^^^^^^^^^^

create
~~~~~~

Used to create a feature type (``SimpleFeatureType``) at the specified zkpath::

    $ geomesa-kafka create -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      -p /geomesa/ds/kafka

describe
~~~~~~~~

Display details about the attributes of a specified feature type::

    $ geomesa-kafka describe -f testfeature -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092 -p /geomesa/ds/kafka

list
~~~~

List all known feature types in Kafka::

    $ geomesa-kafka list -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

If no zkpath parameter is specified, the list command will search all of zookeeper for potential feature types.

listen
~~~~~~

Logs out the messages written to a topic corresponding to the passed in feature type.

    $ geomesa-kafka listen -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka \
      --from-beginning

removeschema
~~~~~~~~~~~~

Used to remove a feature type (``SimpleFeatureType``) in a GeoMesa catalog. This will also delete any feature of that type in the data store::

    $ geomesa-kafka removeschema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka
    $ geomesa-kafka removeschema --pattern 'testfeature\d+' \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka

version
~~~~~~~

Prints out the version, git branch, and commit ID that the tools were built with::

    $ geomesa version