Command Line Tools
==================

This chapter describes GeoMesa Tools, a set of command line tools for feature
management, query planning and explanation, ingest, and export from
the command line.

Installation
------------

See :ref:`setting_up_commandline`.

Installing SFT and Converter Definitions
----------------------------------------

Starting with version 1.2.3, GeoMesa Tools ships with embedded SimpleFeatureType and GeoMesa Conveter definitions for common data types including Twitter, GeoNames, T-drive, and many more. Users can add additional types by providing a ``reference.conf`` file embedded with a jar within the ``lib/common`` directory or by registering the ``reference.conf`` file in the ``$GEOMESA_HOME/conf/sfts`` or ``$GEOMESA_KAKFA_HOME/conf/sfts`` directory.

For example, to add a type named ``customtype``, create a directory named ``$GEOMESA_HOME/conf/sfts/customtype`` and then add the SFT and Conveter typesafe config to the a file named ``$GEOMESA_HOME/conf/sfts/customtype/reference.conf``. This file will be automatically picked up and placed on the classpath when the tools are run.

To see which SFT and Converter confs are installed try the ``geomesa env`` command described below.

Running the command line tools
------------------------------

.. note::

    Some command names have changed in GeoMesa 1.3.0 see :doc:`commandline_old_commands`

Run ``geomesa`` without any arguments to produce the following usage text::

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


This usage text lists the available commands. To see help for an individual command,
run ``geomesa help <command-name>``, which for example will give you something like this::

    $ geomesa help get-names
    List GeoMesa feature types for a given catalog
    Usage: get-names [options]
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

create-schema
~~~~~~~~~~~~~

Used to create a feature type (``SimpleFeatureType``)  in a GeoMesa catalog::

    $ geomesa create -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 \
      -c test_create \
      -f testing \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      --dtg dtg


get-schema
~~~~~~~~~~

Display details about the attributes of a specified feature type::

    $ geomesa get-schema -u username -p password -c test_delete -f testing

get-sft-config
~~~~~~~~~~~~~~

Get the specified feature type as a typesafe config::

    $ geomesa get-sft-config -u username -p password -c test_catalog -f test_feature --format typesafe

Get the specified feature type as an encoded feature schema string::

    $ geomesa get-sft-config -u username -p password -c test_catalog -f test_feature --format spec

keywords
~~~~~~~~

Add or remove keywords to a specified schema::
Repeat the -a or -r flags to add or remove multiple keywords
The ``--removeAll`` option removes all keywords
The ``-l`` option lists the schema's keywords following all operations
If there is whitespace within a keyword, enclose it in quotes for proper functionality::

    $ geomesa keywords -u username -p password \
      -a keywordB -a keywordC -r keywordA -l \
      -i instance -z zoo1,zoo2,zoo3 \
      -c catalog -f featureTypeName

get-names
~~~~~~~~~

List all known feature types in a GeoMesa catalog::

    $ geomesa get-names -u username -p password -c test_catalog

remove-schema
~~~~~~~~~~~~~

Used to remove a feature type (``SimpleFeatureType``) in a GeoMesa catalog. This will also delete any feature of that type in the data store::

    $ geomesa remove-schema -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 \
      -c test_catalog -f testfeature1
    $ geomesa remove-schema -u username -p password \
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

.. note::

    Use the `export-bin` command to export data in binary format.

Example binary export command::

    $ geomesa export-bin -u username -p password \
      -c test_catalog -f test_feature \
      -a "geom,text,user_name" -q "screen_name='JohnSmith'" \
      --id-attribute "id_str" \
      --lat-attribute "coord_lat" \
      --lon-attribute "coord_lon" \
      --date-attribute "dtg" \
      --label-attribute "screen_name"

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
loaded using the `TypeSafe configuration library <https://github.com/typesafehub/config>`__.
They can be referenced by name using the ``-s`` and ``-C`` args.

To define new converters for the users can package a ``reference.conf`` file inside a jar and drop it in the
``$GEOMESA_HOME/lib`` directory or add config definitions to the ``$GEOMESA_TOOLS/conf/application.conf`` file which
includes some examples. SFT and Converter specifications should use the path prefixes
``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>``

For example, here's a simple CSV file to ingest named ``example.csv``::

    FID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

.. note::

    ID is a reserved word, for a full list of reserved words see :ref:`reserved-words`.

To ingest this file, a SimpleFeatureType named ``renegades`` and a converter named ``renegades-csv`` can be placed in
the ``application.conf`` file::

    # cat $GEOMESA_HOME/conf/application.conf
    geomesa {
      sfts {
        renegades = {
          attributes = [
            { name = "fid",      type = "Integer",      index = false                             }
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
          id-field = "toString($fid)"
          fields = [
            { name = "fid",      transform = "$1::int"                 }
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
      -s renegades -C renegades-csv example.csv
    # use the Hadoop file system instead
    $ geomesa ingest -u username -p password \
      -c geomesa_catalog -i instance \
      -s renegades -C renegades-csv hdfs:///some/hdfs/path/to/example.csv

SFT and Converter configs can also be provided as strings or filenames to the ``-s`` and ``-C`` arguments. The syntax is
very similar to the ``application.conf`` and ``reference.conf`` format. Config specifications must be nested using the
paths ``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>`` as shown below::

    # A nested SFT config provided as a string or file to the -s argument specifying
    # a type named "renegades"
    #
    # cat /tmp/renegades.sft
    geomesa.sfts.renegades = {
      attributes = [
        { name = "fid",      type = "Integer",      index = false                             }
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
      id-field = "toString($fid)"
      fields = [
        { name = "fid",      transform = "$1::int"                 }
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
    $ geomesa ingest -u username -p password \
      -c geomesa_catalog -i instance \
      -s /tmp/renegades.sft \
      -C /tmp/renegades.convert hdfs:///some/hdfs/path/to/example.csv


For more documentation on converter configuration, refer to the the ``geomesa-$VERSION/docs/README-convert.md`` file
in the binary distribution.

Shape files may also be ingested::

    $ geomesa ingest -u username -p password \
      -c test_catalog -f shapeFileFeatureName /some/path/to/file.shp


Enabling S3 Ingest
^^^^^^^^^^^^^^^^^^

Hadoop ships with implementations of S3-based filesystems, which can be enabled in the Hadoop configuration used with
GeoMesa tools. Specifically, GeoMesa tools can perform ingests using both the second-generation (`s3n`) and
third-generation (`s3a`) filesystems. Edit the ``$HADOOP_CONF_DIR/core-site.xml`` file in your Hadoop installation,
as shown below (these instructions apply to Hadoop 2.5.0 and higher). Note that you must have the environment variable
``$HADOOP_MAPRED_HOME`` set properly in your environment. Some configurations
can substitute ``$HADOOP_PREFIX`` in the classpath values below.

.. warning::

    AWS credentials are valuable! They pay for services and control read and write protection for data. If you are
    running GeoMesa on AWS EC2 instances, it is recommended to use the ``s3a`` filesystem. With ``s3a``, you can omit the
    Access Key Id and Secret Access keys from `core-site.xml` and rely on IAM roles.

For ``s3a``:

.. code-block:: xml

    <!-- core-site.xml -->
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*</value>
        <description>The classpath specifically for Map-Reduce jobs. This override is needed so that s3 URLs work on Hadoop 2.6.0+</description>
    </property>

    <!-- OMIT these keys if running on AWS EC2; use IAM roles instead -->
    <property>
        <name>fs.s3a.access.key</name>
        <value>XXXX YOURS HERE</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>XXXX YOURS HERE</value>
        <description>Valuable credential - do not commit to CM</description>
    </property>

After you have enabled S3 in your Hadoop configuration you can ingest with GeoMesa tools. Note that you can still
use the Kleene star (*) with S3.:

    $ geomesa ingest -u username -p password -c geomesa_catalog -i instance -s yourspec -C convert s3a://bucket/path/file*

For ``s3n``:

.. code-block:: xml

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

    $ geomesa ingest -u username -p password \
      -c geomesa_catalog -i instance -s yourspec \
      -C convert s3n://bucket/path/file s3n://bucket/path/*


Working with raster data
^^^^^^^^^^^^^^^^^^^^^^^^

delete-raster
~~~~~~~~~~~~~

Delete a given GeoMesa raster table::

    $ geomesa delete-raster -u username -p password -t somerastertable -f

ingest-raster
~~~~~~~~~~~~~

Ingest one or multiple raster image files into Geomesa. Input files, GeoTIFF or
DTED, should be located on the local file system.

.. note::

    Make sure GDAL is installed when doing chunking, which depends on the GDAL utility ``gdal_translate``.

    Input raster files are assumed to have CRS set to EPSG:4326. For non-EPSG:4326 files, they need to be converted into
    EPSG:4326 raster files before ingestion. An example of doing conversion with GDAL utility is ``gdalwarp -t_srs EPSG:4326
    input_file out_file``.

Example usage::

    $ geomesa ingest-raster -u username -p password \
      -t geomesa_raster -f /some/local/path/to/raster.tif

query-rasterstats
~~~~~~~~~~~~~~~~~

Export queries and statistics about the `n` most recent raster queries to a CSV file::

    $ geomesa query-rasterstats -u username -p password -t somerastertable -n 10


Performing system administration tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _add_index_command:

add-index
~~~~~~~~~

Add or update indices for an existing feature type. This can be used to upgrade-in-place, converting an older
index format into the latest. See :ref:`index_upgrades` for more information.

Example usage::

    $ geomesa add-index -u username -p password -i instance \
      -z zoo1,zoo2,zoo3 -c test_catalog -f test_feature --index xz3

delete-catalog
~~~~~~~~~~~~~~

Delete a GeoMesa catalog table completely, along with all features in it.

Example usage::

    $ geomesa delete-catalog -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 -c test_catalog

delete-features
~~~~~~~~~~~~~~~

Delete features from a table in GeoMesa. Does not delete any tables or schema information.

Example usage::

    $ geomesa delete-features -u username -p password \
      -i instance -z zoo1,zoo2,zoo3 -c test_catalog \
      -q 'dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'

add-attribute-index
~~~~~~~~~~~~~~~~~~~

Add an attribute index for a specified list of attributes.::

    $ geomesa add-attribute-index -u username -p password -i instance -z zoo1,zoo2,zoo3 -c test_catalog \
        -f test_feature -a attribute1,attribute2 --coverage full

env
~~~

Examines the current GeoMesa tools environment, and prints out simple feature types converters that
are available on the current classpath. The available types can be used for ingestion; see the :ref:`ingest` command.
Use of this command without parameters will result in behavior similar to when the help command is used.

Parameters allow you to specify what to print to out. These give you the ability to view a list of all simple
feature types and converters, describe all the feature types and converters, or review a subset of these simple feature
types and converters. There are a few options that permit you to specify the desired format when describing simple
feature types.

There are a few commands pertaining to the format of describing simple feature types.

Example usage::

    $ geomesa env --list-sfts

explain
~~~~~~~

Explain how a given GeoMesa query will be executed::

    $ geomesa explain -u username -p password \
      -c test_catalog -f test_feature \
      -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

stats-analyze
~~~~~~~~~~~~~

Analyze statistics for your data set. This may improve query planning.

Example usage::

    $ geomesa stats-analyze -u username -p password -c geomesa.data -f twitter
      Running stat analysis for feature type twitter...
      Stats analyzed:
        Total features: 8852601
        Bounds for geom: [ -171.75, -45.5903996, 157.7302, 89.99997102 ] cardinality: 2119237
        Bounds for dtg: [ '2016-02-01T00:09:12.000Z' to '2016-03-01T00:21:02.000Z' ] cardinality: 2161132
        Bounds for user_id: [ '100000215' to '99999502' ] cardinality: 861283
      Use 'stats-histogram' or 'stats-count' commands for more details

stats-bounds
~~~~~~~~~~~~

Displays the bounds of your data for different attributes. You can use pre-calculated stats for a quick
estimation, or get the definitive result by querying the data set using the '--no-cache' flag.

Example usage::

    $ geomesa stats-bounds -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter
      user_id [ 100000215 to 99999502 ] cardinality: 861283
      user_name [ unavailable ]
      text [ unavailable ]
      dtg [ 2016-02-01T00:09:12.000Z to 2016-03-01T00:21:02.000Z ] cardinality: 2161132
      geom [ -171.75, -45.5903996, 157.7302, 89.99997102 ] cardinality: 2119237

    $ geomesa stats-bounds -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter --no-cache \
        -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Running stat query...
        user_id [ 1011811424 to 99124417 ] cardinality: 115
        user_name [ bar_user to foo_user ] cardinality: 113
        text [ bar to foo ] cardinality: 180
        dtg [ 2016-02-02T00:01:07.000Z to 2016-02-02T23:59:41.000Z ] cardinality: 178
        geom [ -69.87212338, 45.01259299, -60.08925, 53.8868369 ] cardinality: 155

stats-count
~~~~~~~~~~~

Counts the features in your data set. You can count total features, or features that match a CQL filter.
You can use pre-calculated stats for a quick estimation, or get the definitive result by querying the
data set using the '--no-cache' flag.

Example usage::

    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter
      Estimated count: 8852601

    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter \
        -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Estimated count: 2681

    $ geomesa stats-count -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter --no-cache \
        -q 'BBOX(geom,-70,45,-60,55) AND dtg DURING 2016-02-02T00:00:00.000Z/2016-02-03T00:00:00.000Z'
      Running stat query...
      Count: 182


stats-top-k
~~~~~~~~~~~

Enumerates the values for attributes in your data set. You can enumerate all values for all features,
or only values for features that match a CQL filter.

Example usage::

    $ geomesa stats-top-k -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter -a user_id -k 10
      Top values for 'user_id':
        3144822634 (26681)
        388009236 (20553)
        497145453 (19858)
        563319506 (15848)
        2841269945 (15763)
        2924224280 (15731)
        141302910 (15240)
        2587789764 (14811)
        56266341 (14487)
        889599440 (14330)

stats-histogram
~~~~~~~~~~~~~~~

Counts the features in your data set, grouped into sorted bins. You may specify the number of bins to group
attribute into. You can count total features, or features that match a CQL filter. You can use
pre-calculated stats for a quick estimation, or get the definitive result by querying the
data set using the '--no-cache' flag.

If you query a histogram for a geometry attribute, the result will be displayed in an ASCII heatmap.

Example usage::

    $ geomesa stats-histogram -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter -a dtg --bins 10
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

    $ geomesa stats-histogram -u username -p password -i instance -z zoo1,zoo2,zoo3 \
        -c geomesa.data -f twitter -a dtg --bins 10 --no-cache
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

config-table
~~~~~~~~~~~~

Perform various table configuration tasks. There are three sub-arguments:

 * **list** - List the configuration options for a GeoMesa table
 * **describe** - Describe a given configuration option for a table
 * **update** - Update a given configuration option for a table

Example commands::

    $ geomesa config-table list -u username -p password \
      -c test_catalog -f test_feature -t st_idx
    $ geomesa config-table describe -u username -p password \
      -c test_catalog -f test_feature -t attr_idx \
      --param table.bloom.enabled
    $ geomesa config-table update -u username -p password \
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
          create-schema   Create a feature definition in GeoMesa
          get-schema      Describe the attributes of a given feature in GeoMesa
          get-names       List GeoMesa features for a given zkPath
          help            Show help
          listen          Listen to a GeoMesa Kafka topic
          remove-schema   Remove a schema and associated features from GeoMesa
          version         GeoMesa Version

This usage text lists the available commands. To see help for an individual command,
run ``geomesa-kafka help <command-name>``, which for example will give you something like this::

    $ geomesa-kafka help get-names
      List GeoMesa features for a given zkPath
      Usage: get-names [options]
        Options:
        * -b, --brokers
             Brokers (host:port, comma separated)
          -p, --zkpath
             Zookeeper path where feature schemas are saved
        * -z, --zookeepers
             Zookeepers (host[:port], comma separated)

Command overview
^^^^^^^^^^^^^^^^

create-schema
~~~~~~~~~~~~~

Used to create a feature type (``SimpleFeatureType``) at the specified zkpath::

    $ geomesa-kafka create-schema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
      -p /geomesa/ds/kafka

get-schema
~~~~~~~~~~

Display details about the attributes of a specified feature type::

    $ geomesa-kafka get-schema -f testfeature -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 -p /geomesa/ds/kafka

get-names
~~~~~~~~~

List all known feature types in Kafka::

    $ geomesa-kafka get-names -z zoo1,zoo2,zoo3 -b broker1:9092,broker2:9092

If no ``--zkpath`` parameter is specified, the ``get-names`` command will search all of zookeeper for potential feature types.

listen
~~~~~~

Logs out the messages written to a topic corresponding to the feature type passed in.

    $ geomesa-kafka listen -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka \
      --from-beginning

remove-schema
~~~~~~~~~~~~~

Used to remove a feature type (``SimpleFeatureType``) in a GeoMesa catalog. This will also delete any feature of that type in the data store::

    $ geomesa-kafka remove-schema -f testfeature \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka
    $ geomesa-kafka remove-schema --pattern 'testfeature\d+' \
      -z zoo1,zoo2,zoo3 \
      -b broker1:9092,broker2:9092 \
      -p /geomesa/ds/kafka

version
~~~~~~~

Prints out the version, git branch, and commit ID that the tools were built with::

    $ geomesa version
