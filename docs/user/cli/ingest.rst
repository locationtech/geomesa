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

========================== ==================================================================================================
Argument                   Description
========================== ==================================================================================================
``-c, --catalog *``        The catalog table containing schema metadata
``-f, --feature-name``     The name of the schema
``-s, --spec``             The ``SimpleFeatureType`` specification to create
``-C, --converter``        The GeoMesa converter used to create ``SimpleFeature``\ s
``--converter-error-mode`` Override the error mode defined by the converter
``-t, --threads``          Number of parallel threads used
``--input-format``         Format of input files (csv, tsv, avro, shp, json, etc)
``--no-tracking``          This application closes when ingest job is submitted. Useful for launching jobs with a script
``--run-mode``             Must be one of ``local``, ``distributed``, or ``distributedcombine``
``--split-max-size``       Maximum size of a split in bytes (distributed jobs)
``--src-list``             Input files are text files with lists of files, one per line, to ingest
``--force``                Suppress any confirmation prompts
``<files>...``             Input files to ingest
========================== ==================================================================================================

The ``--converter`` argument may be any of the following:

* The name of a GeoMesa converter already available on the classpath
* A converter configuration string
* The name of a file containing a converter configuration

If a converter is not specified, GeoMesa will attempt to infer a converter definition based on the input files.
Currently this supports GeoJSON, self-describing Avro, delimited text (TSV, CSV) or Shapefiles. If GeoMesa is able
to infer a schema and converter definition, the user can accept them as-is, or alternatively use them as the basis
for a fully custom converter. If desired, the user can persist the inferred converter to file, which allows for
easy modification and reuse. When ingesting a large data set, it can be useful to ingest a single file in local
mode, using schema inference to generate the converter. The converter definition can be persisted and tweaked to
satisfaction, then used for the entire data set with a distributed ingest.

See :ref:`cli_converter_conf` for more details on specifying the converter.

The ``converter-error-mode`` argument may be used to override the error mode defined in the converter. It must be
one of ``skip-bad-records`` or ``raise-errors``.

If the ``--feature-name`` is specified and the schema already exists, then ``--spec`` is not required. Likewise,
if a converter is not defined, the schema will be inferred alongside the converter. Otherwise, ``--spec`` may be
any of the following:

* A string of attributes, for example ``name:String,dtg:Date,*geom:Point:srid=4326``
* The name of a ``SimpleFeatureType`` already available on the classpath
* A string of attributes, defined as a TypeSafe configuration
* The name of a file containing one of the above

If the schema doesn't exist, the ``--feature-name`` argument is required if it is not implied by
the specification string. It may also be used to override the implied feature name.

See :ref:`cli_sft_conf` for more details on specifying the ``SimpleFeatureType``.

The ``--input-format`` argument can be used to specify the type of files being ingested. Currently
GeoMesa supports Avro, CSV, TSV, Json/GeoJson, GML, and SHP. If not specified, the input file extensions
will be used to determine the file type.

The ``--no-tracking`` argument instructs the application to close when the ingest job has been submitted rather than
tracking and displaying the progress of the ingest. This is useful when a script is submitting the job or it is
undesirable to leave the JVM running. Note that supplying this parameter does not silence the application and it will
still provide information about the status of the job submission.

The ``--run-mode`` argument can be used to run ingestion locally or distributed (using map/reduce). Note that in
order to run in distributed mode, the input files must be in HDFS. By default, input files on the local filesystem
will be ingested in local mode, and input files in HDFS will be ingested in distributed mode. If using the
``distributedcombine`` mode, multiple files will be processes by each mapper up to the limit specified by
``--split-max-size``.

The ``--threads`` argument can be used to increase local ingest speed. However, there can not be more threads
than there are input files. The ``--threads`` argument is ignored for distributed ingest.

The ``--split-max-size`` argument can be used to control the amount of data each mapper processes. This is useful
when used in conjunction with the DistributedCombine ``--run-mode`` and if input files are small or starting a mapper
for each one becomes prohibitively slow. For example, if you have 100 5MB files then a setting of 100000000 (100MB)
would schedule 5 mappers.

.. _src-list:

The ``--src-list`` argument is useful when you have more files to ingest than the command line will allow you to
specify. This file instructs GeoMesa to treat input files as new-line-separated file lists. As this makes it very
easy to run ingest jobs that can take days it's recommended to split lists into reasonable chunks that can be completed
in a couple hours.

The ``--force`` argument can be used to suppress any confirmation prompts (generally from converter inference),
which can be useful when scripting commands.

The ``<files>...`` argument specifies the files to be ingested. ``*`` may be used as a wild card in file paths.
GeoMesa can handle **gzip**, **bzip** and **xz** file compression as long as the file extensions match the
compression type. GeoMesa supports ingesting files from local disks or HDFS. In addition, Amazon's S3
and Microsoft's Azure file systems are supported with a few configuration changes. See
:doc:`/user/cli/filesystems` for details. Note: The behavior of this argument is changed by the ``--src-list`` argument.

Instead of specifying files, input data may be piped directly to the ingest command using `stdin` shell redirection.
Note that this will only work in local mode, and will only use a single thread for ingestion. Schema inference is
disabled in this case, and progress indicators may not be entirely accurate, as the total size isn't known up front.
For example::

    cat foo.csv | geomesa-accumulo ingest ...

For local ingests, feature writers will be pooled and only flushed periodically. The frequency of flushes can be
controlled via the system property ``geomesa.ingest.local.batch.size``, and defaults to every 20,000 features.
