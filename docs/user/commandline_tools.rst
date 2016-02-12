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

Run the ``geomesa`` without any arguments to produce the following usage text::

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

A test script is included under ``geomesa-tools-$VERSION/bin/test-geomesa`` that runs each
command provided by geomesa-tools. Edit this script by including your Accumulo
username, password, test catalog table, test feature name, and test SFT
specification. Default values are already included in the script. Then, run the
script from the command line to ensure there are no errors in the output text. 

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

Get the specified feature type as an encoded feature schema string::

    $ geomesa getsft -u username -p password -c test_catalog -f test_feature

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
            { name = "lastseen", transform = "$4::date"                }
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
very similar to the ``application.conf`` and ``reference.conf`` format. Config specifications can be nested using the
paths ``geomesa.converters.<convertername>`` and ``geomesa.sfts.<typename>`` or non-nested. If you provide a non nested
sft config you must provide the field ``type-name``. For example::

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
    }

    # cat /tmp/renegades.sft
    # a non-nested config for the -s argument that specifying a type named "renegades"
    {
      type-name  = "renegades"
      attributes = [
        { name = "id",       type = "Integer",      index = false                             }
        { name = "name",     type = "String",       index = true                              }
        { name = "age",      type = "Integer",      index = false                             }
        { name = "lastseen", type = "Date",         index = true                              }
        { name = "friends",  type = "List[String]", index = true                              }
        { name = "geom",     type = "Point",        index = true, srid = 4326, default = true }
      ]
    }

Similarly, converter configurations can be nested or not when passing them directly to the ``-C`` argument::

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
        { name = "lastseen", transform = "$4::date"                }
        { name = "friends",  transform = "parseList('string', $5)" }
        { name = "lon",      transform = "$6::double"              }
        { name = "lat",      transform = "$7::double"              }
        { name = "geom",     transform = "point($lon, $lat)"       }
      ]
    }

    # a non-nested converter definition
    # cat /tmp/renegades.convert
    {
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
        { name = "lastseen", transform = "$4::date"                }
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
