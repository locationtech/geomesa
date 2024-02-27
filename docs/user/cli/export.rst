Query and Export Commands
=========================

These commands are used to query and export simple features. Required parameters are indicated with a ``*``.

``convert``
-----------

The convert command is used to directly transform data in one format into another, without ingesting them
into GeoMesa. For example, it could be used to convert CSV files to GeoJSON.

========================== ===================================================================================
Argument                   Description
========================== ===================================================================================
``-f, --feature-name``     The name of the schema
``-s, --spec``             The ``SimpleFeatureType`` specification to create
``-C, --converter``        The GeoMesa converter used to create ``SimpleFeature``\ s
``--converter-error-mode`` Override the error mode defined by the converter
``-q, --cql``              CQL filter to select features to export
``-m, --max-features``     Limit the number of features exported
``-F, --output-format``    Output format used for export
``-o, --output``           Output to a file instead of standard out
``--input-format``         File format of input files (shp, csv, tsv, avro, etc)
``--hints``                Query hints used to modify the query
``--gzip``                 Level of gzip compression to use for output, from 1-9
``--no-header``            Don't export the type header, for CSV and TSV formats
``--src-list``             Input files are text files with lists of files, one per line, to ingest
``--suppress-empty``       If no features are converted, don't write any headers or other output
``--force``                Force execution without prompt
========================== ===================================================================================

See :ref:`cli_export` and :ref:`cli_ingest` for a description of the arguments.

.. _cli_explain:

``explain``
-----------

The explain command can be used to debug queries that are slow or problematic. Without actually running the query,
it will show a variety of data, including the index being used, any query hints extracted, the exact ranges being
scanned and filters being applied.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql *``          CQL filter to select features to export
``-a, --attributes``     Specific attributes to export
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
======================== =========================================================

See :ref:`cli_export` for a description of the arguments.

.. _cli_export:

``export``
----------

Export features in a variety of formats.

See :ref:`data_migration` for details on how the export/import commands can be used to move data between clusters.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``-a, --attributes``     Comma-separated list of attributes to export
``--attribute``          Complex attribute transforms to export
``-m, --max-features``   Limit the number of features exported
``-F, --output-format``  Output format used for export
``-o, --output``         Output to a file instead of standard out
``--sort-by``            Sort by the specified attributes
``--sort-descending``    Sort in descending order, instead of the default ascending order
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
``--no-header``          For CSV and TSV formats, suppress the normal column headers
``--suppress-empty``     If no features are exported, don't write any headers or other output
``--gzip``               Level of gzip compression to use for output, from 1-9
``--chunk-size``         Split the output into multiple files of the given size
``--run-mode``           Run locally or as a distributed map/reduce job
``--num-reducers``       Specify the number of reduces, when running in distributed mode
``--force``              Force execution without prompts
======================== =========================================================

The ``--attributes`` and ``--attribute`` parameters can be used to select a subset of attributes to export, or
to transform the returned attributes using filter functions. A simple projection can be specified as a
comma-delimited list::

    --attributes name,age,geom

The feature ID can be specified along with other attribute names, with the reserved word ``id``::

    --attributes id,name,age,geom

Transforms can be accomplished by using ``--attribute`` to specify transform functions. Note that compared to
``--attributes``, only a single argument can be specified per parameter, and commas will not be interpreted as
a delimiter::

    --attribute id --attribute name --attribute "name_transform=strConcat(name, 'foo')"

For a full list of transforms, see the GeoTools `documentation <https://docs.geotools.org/latest/userguide/library/main/function_list.html>`_.
Note that not all functions work in transforms, however.

The ``--output-format`` argument defines the encoding used for export. Currently, it can be one of:

* ``arrow`` Apache Arrow streaming file format
* ``avro`` Apache Avro format
* ``bin`` Custom minimal binary encoding
* ``csv`` or ``tsv``
* ``json``
* ``gml`` or ``gml2`` `Geography Markup Language <https://www.opengeospatial.org/standards/gml>`_
* ``html`` Export data to a Leaflet map and open in the default browser, if possible
* ``orc`` Apache Orc files
* ``parquet`` Apache Parquet files
* ``shp`` ESRI Shapefile
* ``null`` suppress output entirely

.. note::

    The Leaflet format is intended for testing and small scale data exploration and visualization only. For production
    map generation it is highly recommended to use GeoServer. Additionally, the resulting file from this command
    requires the use of an online browser to open in order to access online resources.

The ``--output`` argument can be used to export to a file. By default, export data is written to standard output.

The ``--sort-by`` argument can be used to sort the output by one or more attributes. Note that for local exports,
this will usually be done in-memory, so can be costly for large result sets. By default, output is sorted in
ascending order; ``--sort-descending`` can be used to reverse the sort order.

The ``--hints`` argument can be used to set query hints. Hints should be specified as ``key1=value1;key2=value2``, etc.
Note that due to shell expansion, the hint string will likely need to be quoted. See :ref:`analytic_queries` for
examples of query hints that can be set. Note that query hints may cause errors if they don't correspond to the
output format specified.

The ``--index`` argument can be used to force the query to run against a particular index, instead of using
the best index as determined through query planning. The argument should be the name of an index, e.g. ``id``
or ``z3``. See :ref:`index_overview` for a list of valid indices. Note that not all schemas will
have all index types.

The ``--gzip`` argument can be used to compress the output through **gzip** encoding. It can be specified
as a number between 1-9. Higher numbers indicate more compression, lower numbers indicate faster compression.

The ``--chunk-size`` argument can be used to split the output into multiple smaller files. It is specified as
a size in bytes, e.g. ``10MB``. Note that due to buffering and metadata, it is generally not feasible to hit
the chunk size exactly. For finer control, the system property ``org.locationtech.geomesa.export.bytes-per-feature``
can be used to set an initial number of bytes per feature as a float, i.e. ``5.5``. If exporting to multiple
formats at once, the property  ``org.locationtech.geomesa.export.<name>.bytes-per-feature`` can also be used,
where ``<name>`` corresponds to one of  the output formats mentioned above (e.g. ``json``, ``parquet``, etc).

The ``--run-mode distributed`` argument can be used to specify a distributed map/reduce export, for data stores
that support it. Distributed exports can be used for data migration or large-scale sorting. When running
in distributed mode, the output file must be in a distributed filesystem (e.g. HDFS or S3). When sorting a
distributed export, ``--num-reducers`` must be specified to set the number of reducer tasks.

The ``--force`` argument can be used to suppress prompts, such as a prompt to overwrite an existing file. This
should be used with care, as data loss can occur if you export to an existing directory.

``playback``
------------

The playback command can simulate a streaming ingestion by replaying features that have already been ingested.
Features are returned based on a date attribute in the feature. For example, if replaying three features that
have dates that are each one second apart, each feature will be emitted after a delay of one second. The rate
of export can be modified to speed up or slow down the original time differences.

In order to simulate a data stream, the output of this command can be piped into another process, for example
to send messages to a Kafka topic or write files to NiFi for ingestion.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``--interval *``         Date interval to replay, in the format
                         ``yyyy-MM-dd'T'HH:mm:ss.SSSZ/yyyy-MM-dd'T'HH:mm:ss.SSSZ``
``--dtg``                Date attribute to base playback on. If not specified,
                         will use the default schema date field
``--rate``               Rate multiplier to speed-up (or slow down) features being
                         returned, as a float
``--live``               Will modify the returned dates to match the current time
``--step-window``        Query the interval in discrete chunks instead of all at
                         once ('10 minutes', '30 seconds', etc)
``-q, --cql``            Additional CQL filter to select features to export.
                         Features will automatically be filtered to match the
                         time interval
``-a, --attributes``     Specific attributes to export
``-m, --max-features``   Limit the number of features exported
``-F, --output-format``  Output format used for export
``-o, --output``         Output to a file instead of standard out
``--hints``              Query hints used to modify the query
``--no-header``          Don't export the type header, for CSV and TSV formats
``--suppress-empty``     If no features are exported, don't write any headers or other output
``--gzip``               Level of gzip compression to use for output, from 1-9
======================== =========================================================

The playback command is an extension of the :ref:`cli_export` command, and accepts all the parameters outlined there.

The ``--interval`` parameter specifies the date range for features to replay, based on the date attribute
specified by ``--dtg``, or the default schema date attribute if not specified.

The ``--rate`` parameter can be used to speed up or slow down the replay. It is specified as a floating point
number. For example ``--rate 10`` will make replay ten times faster, while ``--rate 0.1`` will make replay
ten times slower.

The ``--step-window`` parameter can be used to break up the query into discrete chunks, based on the time interval.
For larger exports, this will save memory overhead when sorting and will likely be faster. The window should be
large enough so that the overhead of creating multiple queries doesn't slow down the process, but small enough so
that a manageable batch of features is returned for each query. The optimal window size will depend on the
time-based density of features, and the available hardware.
