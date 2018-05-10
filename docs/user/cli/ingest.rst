Ingest Commands
===============

These commands are used to insert and delete simple features. Required parameters are indicated with a ``*``.

``delete-features``
-------------------

Delete specific features from a schema. Note that if deleting all features, it may be faster to delete the
schema and re-create it.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to delete
======================== =========================================================

.. _cli_ingest:

``ingest``
----------

The ingest command takes files in various formats and ingests them as ``SimpleFeature``\ s in GeoMesa.
Generally, a GeoMesa 'converter' definition is required to map input data to  ``SimpleFeature``\ s. GeoMesa
supports common input formats such as delimited text (TSV, CSV), fixed width files, JSON, XML, and Avro.
The converter framework is extensible via Java SPI, to allow support for custom formats. See
:ref:`converters` for more information on converters.

See :ref:`data_migration` for details on how the export/import commands can be used to move data between clusters.

====================== =========================================================
Argument               Description
====================== =========================================================
``-c, --catalog *``    The catalog table containing schema metadata
``-C, --converter``    The GeoMesa converter used to create ``SimpleFeature``\ s
``-s, --spec``         The ``SimpleFeatureType`` specification to create
``-f, --feature-name`` The name of the schema
``-t, --threads``      Number of parallel threads used
``--input-format``     Format of input files (csv, tsv, avro, shp, json, etc)
``--run-mode``         Must be one of ``distributed`` or ``local``
``<files>...``         Input files to ingest
====================== =========================================================

The ``--converter`` argument may be any of the following:

* The name of a GeoMesa converter already available on the classpath
* A converter configuration string
* The name of a file containing a converter configuration

See :ref:`cli_converter_conf` for more details on specifying the converter.

If a converter is not specified, GeoMesa will attempt to extract a schema from the input files. Currently
this only works in a very limited fashion - the input must be a Shapefile, self describing Avro, or
delimited text (TSV, CSV) with a header row that defines the attribute name and type of each column. For example::

    id,name:String,dtg:Date,*geom:Point:srid=4326
    1,foo,2017-01-01T00:00:00.000Z,POINT(45 55)
    2,bar,2017-01-02T00:00:00.000Z,POINT(55 65)

If the ``--feature-name`` is specified and the schema already exists, then ``--spec`` is not required. Otherwise,
``--spec`` may be any of the following:

* A string of attributes, for example ``name:String,dtg:Date,*geom:Point:srid=4326``
* The name of a ``SimpleFeatureType`` already available on the classpath
* A string of attributes, defined as a TypeSafe configuration
* The name of a file containing one of the above

See :ref:`cli_sft_conf` for more details on specifying the ``SimpleFeatureType``.

If the schema doesn't exist, the ``--feature-name`` argument is required if it is not implied by
the specification string. It may also be used to override the implied feature name.

The ``--input-format`` argument can be used to specify the type of files being ingested. Currently
GeoMesa supports Avro, CSV, TSV, Json/GeoJson, GML, and SHP. If not specified, the input file extensions
will be used to determine the file type.

The ``--run-mode`` argument can be used to run ingestion locally or distributed (using map/reduce). Note that in
order to run in distributed mode, the input files must be in HDFS. By default, input files on the local filesystem
will be ingested in local mode, and input files in HDFS will be ingested in distributed mode.

The ``--threads`` argument can be used to increase local ingest speed. However, there can not be more threads
than there are input files. The ``--threads`` argument is ignored for distributed ingest.

The ``<files>...`` argument specifies the files to be ingested. ``*`` may be used as a wild card in file paths.
GeoMesa can handle **gzip**, **bzip** and **xz** file compression as long as the file extensions match the
compression type. GeoMesa supports ingesting files from local disks or HDFS. In addition, Amazon's S3
and Microsoft's Azure file systems are supported with a few configuration changes. See
:doc:`/user/cli/filesystems` for details.

Instead of specifying files, input data may be piped directly to the ingest command using `stdin` shell redirection.
Note that this will only work in local mode, and will only use a single thread for ingestion. Progress indicators
may not be entirely accurate, as the total size isn't known up front. For example::

    cat foo.csv | geomesa-accumulo ingest ...
