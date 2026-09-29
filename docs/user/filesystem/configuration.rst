.. _fsds_config_props:

FileSystem DataStore Configuration
==================================

Iceberg Configuration
---------------------

Iceberg-specific configuration properties are configured through the :ref:`fsds_parameters` ``fs.config.properties`` and
``fs.config.file``. See the `Apache Iceberg documentation <https://iceberg.apache.org/docs/latest/configuration/#table-properties>`__
for details on the available configuration properties. See also :ref:`fsds_metadata` for details on configuring the Iceberg Catalog.

Storage Configuration Properties
--------------------------------

Storage-specific configuration properties are configured through the :ref:`fsds_parameters` ``fs.config.properties`` and
``fs.config.file``. Additional properties related to metadata storage are outlined in :ref:`fsds_metadata`.

fs.writer.partitions.max.open
+++++++++++++++++++++++++++++

When writing to multiple partitions at once, this restricts the maximum number of partition files to hold open at one time, per
writer. It can also be specified through :ref:`fsds_parameters`, which will take precedence.

.. _fsds_parquet_geometries_prop:

geomesa.parquet.geometries
++++++++++++++++++++++++++

This property can be used to control the encoding schema used for geometry-type columns when using Parquet. The available options
are:

* ``GeoParquetWkb`` (default) - This schema uses `GeoParquet 1.1.0 <https://geoparquet.org/releases/v1.1.0/>`__ with geometries
  encoded as WKB. This format is supported by most 3rd party libraries that can read GeoParquet.
* ``GeoParquetNative`` - This schema uses `GeoParquet 1.1.0 <https://geoparquet.org/releases/v1.1.0/>`__ with geometries
  encoded "natively". This format doesn't require special libraries to read, but isn't as widely supported as WKB.

parquet.writer.version
++++++++++++++++++++++

Specifies the file version to write, either ``v2`` (default) or ``v1``.

parquet.compression
+++++++++++++++++++

Specifies the file compression to use, by default ``ZSTD``.

parquet.block.size
++++++++++++++++++

Specifies the target row group size of written files, either in bytes or with a suffix such as ``64MB``. The default is
Parquet's own (``128MB``). A writer buffers an entire row group in memory, and up to ``fs.writer.partitions.max.open``
writers may be open at once, so smaller values reduce memory use when writing many partitions. However, every row group adds
metadata for each column to the file footer, so small row groups in large files of wide schemas produce large footers, which
some readers refuse (Trino, for example, rejects footers over 15MB by default). Can also be set as a system property, which
the storage configuration overrides.

Parquet writer properties
+++++++++++++++++++++++++

In addition to the options above, the standard Parquet writer properties (as defined by Parquet's ``ParquetOutputFormat``)
are applied when writing, for example ``parquet.page.size``, ``parquet.dictionary.page.size``, ``parquet.enable.dictionary``,
``parquet.page.row.count.limit``, ``parquet.column.statistics.enabled`` and ``parquet.page.write-checksum.enabled``. Properties
that Parquet supports per column (dictionary encoding, statistics, bloom filters, and compression and compression level) can be
applied to a single column by appending ``#`` and the column name, e.g. ``parquet.enable.dictionary#name=false`` or
``parquet.compression#name=SNAPPY``. Properties that aren't set use Parquet's defaults, and invalid values are ignored with a
warning. Compression codec settings (e.g. ``parquet.compression.codec.zstd.level``) are also passed through to the codec.

parquet.bloom.filter.*
++++++++++++++++++++++

Writes Parquet bloom filters, which let readers skip row groups on equality predicates against high-cardinality columns
(such as unique keys) where min/max statistics can't prune. The options are Parquet's own, applied either to all columns or
to a single column by appending ``#`` and the column name:

* ``parquet.bloom.filter.enabled[#<column>]`` - ``true`` or ``false``, by default ``false``
* ``parquet.bloom.filter.expected.ndv#<column>`` - the expected number of distinct values per row group, used to size the filter
* ``parquet.bloom.filter.fpp#<column>`` - the false positive probability, by default ``0.01``
* ``parquet.bloom.filter.max.bytes`` - the maximum size of each filter, in bytes or with a suffix such as ``64k``, by default ``1MB``
* ``parquet.bloom.filter.adaptive.enabled`` - size each filter from the values actually written, instead of the expected ndv
* ``parquet.bloom.filter.candidates.number#<column>`` - the number of candidate sizes to try when adaptive, by default ``5``

A filter is written per column per row group. Without an expected ndv or adaptive sizing, each filter is the maximum size
(``1MB`` by default), regardless of the number of values in the row group. Adaptive candidates halve in size from the
maximum, so the smallest filter adaptive sizing will choose is ``max.bytes / 2^(candidates - 1)`` (``64k`` by default).
Parquet does not write a filter for a column chunk that is entirely dictionary encoded, as the dictionary already answers
membership.

When using Iceberg, the table properties ``write.parquet.bloom-filter-enabled.column.<column>``,
``write.parquet.bloom-filter-ndv.column.<column>``, ``write.parquet.bloom-filter-fpp.column.<column>`` and
``write.parquet.bloom-filter-max-bytes`` are also honored, and take precedence over the storage configuration. These are
set when a schema is created with bloom filters (see :ref:`fsds_bloom_filter_config`), and are the same properties used by
other Iceberg writers (for example Trino's ``parquet_bloom_filter_columns`` table property), so files rewritten by
compaction keep the same filters.

AWS S3 Configuration
--------------------

The following properties are specific to S3, and are also specified through the :ref:`fsds_parameters` ``fs.config.properties``
and ``fs.config.file``. Additionally, the `AWS Java SDK <https://docs.aws.amazon.com/sdk-for-java/>`__ (v2) will load
configuration from various places, such as ``~/.aws/config``, ``~/.aws/credentials`` or various environment variables. Many of
these parameters map directly to the underlying S3 client configuration. See the AWS documentation for details.

fs.s3.region
++++++++++++

Override the default S3 region. Can also be specified through the environment variable ``AWS_REGION``.

fs.s3.access-key-id
+++++++++++++++++++

Authenticate with this AWS access key. Can also be specified through the environment variable ``AWS_ACCESS_KEY_ID``.

.. warning::

    AWS credentials are valuable - make sure to safeguard them appropriately.

fs.s3.secret-access-key
+++++++++++++++++++++++

Authenticate with this AWS secret access key. Can also be specified through the environment variable ``AWS_SECRET_ACCESS_KEY``.

.. warning::

    AWS credentials are valuable - make sure to safeguard them appropriately.

fs.s3.endpoint
++++++++++++++

Override the default S3 endpoint url. Can also be specified through the environment variable ``FS_S3_ENDPOINT``.

fs.s3.force-path-style
++++++++++++++++++++++

Force "path-style" access, useful for connecting to non-AWS stores such as Minio. Can also be specified through the environment
variable ``FS_S3_FORCE_PATH_STYLE``.

fs.s3.write-buffering
+++++++++++++++++++++

Specify the buffering strategy for writes to S3. Must be one of ``disk`` (default) or ``memory``. Data will be written to the
specified location until it hits a certain threshold, at which point it will be asynchronously uploaded to S3.
Can also be specified through the environment variable ``FS_S3_WRITE_BUFFERING``.

fs.s3.write-buffer-dir
++++++++++++++++++++++

When using ``disk`` buffering, specify the directory to use for intermediate writes, default ``${java.io.tmpdir}/s3/``.
Can also be specified through the environment variable ``FS_S3_WRITE_BUFFER_DIR``.

fs.s3.write-buffer-in-bytes
+++++++++++++++++++++++++++

Specify the amount of data to buffer before uploading to S3, default ``64MB``. Can also be specified through the environment
variable ``FS_S3_WRITE_BUFFER_IN_BYTES``.

fs.s3.num-retries
+++++++++++++++++

Specify the number of retries in the S3 client. Can also be specified through the environment variable ``FS_S3_NUM_RETRIES``.

fs.s3.target-throughput-in-gbps
+++++++++++++++++++++++++++++++

Specify the target throughput in the S3 client. Can also be specified through the environment variable
``FS_S3_TARGET_THROUGHPUT_IN_GBPS``.

fs.s3.minimum-part-size-in-bytes
++++++++++++++++++++++++++++++++

Specify the minimum part size in the S3 client. Can also be specified through the environment variable
``FS_S3_MINIMUM_PART_SIZE_IN_BYTES``.

fs.s3.max-concurrency
+++++++++++++++++++++

Specify the maximum concurrency in the S3 client. GeoMesa overrides the default to be ``600``. Can also be specified through the
environment variable ``FS_S3_MAX_CONCURRENCY``.

fs.s3.connection-timeout
++++++++++++++++++++++++

Specify the connection timeout in the S3 client. Can also be specified through the environment variable
``FS_S3_CONNECTION_TIMEOUT``.

fs.s3.max-native-memory-limit-in-bytes
++++++++++++++++++++++++++++++++++++++

Specify the maximum native memory limit in the S3 client. Can also be specified through the environment variable
``FS_S3_MAX_NATIVE_MEMORY_LIMIT_IN_BYTES``.

fs.s3.request-checksum-calculation
++++++++++++++++++++++++++++++++++

Specify the checksum calculation in the S3 client. Valid values are ``WHEN_SUPPORTED`` or ``WHEN_REQUIRED``. Can also be
specified through the environment variable ``FS_S3_REQUEST_CHECKSUM_CALCULATION``.

fs.s3.response-checksum-validation
++++++++++++++++++++++++++++++++++

Specify the checksum calculation in the S3 client. Valid values are ``WHEN_SUPPORTED`` or ``WHEN_REQUIRED``. Can also be
specified through the environment variable ``FS_S3_RESPONSE_CHECKSUM_VALIDATION``.

fs.s3.initial-read-buffer-size-in-bytes
+++++++++++++++++++++++++++++++++++++++

Specify the initial read buffer size in the S3 client. Can also be specified through the environment variable
``FS_S3_INITIAL_READ_BUFFER_SIZE_IN_BYTES``.

fs.s3.accelerate
++++++++++++++++

Enable the S3 client to use S3 Transfer Acceleration endpoints, default ``false``. Can also be specified through the environment
variable ``FS_S3_ACCELERATE``.

fs.s3.threshold-in-bytes
++++++++++++++++++++++++

Specify the threshold in the S3 client. Can also be specified through the environment variable ``FS_S3_THRESHOLD_IN_BYTES``.

System Properties
-----------------

This section details configuration properties specific to the FileSystem data store. For general properties,
see :ref:`runtime_configuration`.

geomesa.fs.file.cache.duration
++++++++++++++++++++++++++++++

To avoid repeated reads from disk, GeoMesa will cache the results of disk operations for a certain period of time.
This has the side effect that files modified by external processes may not be visible until after the cache timeout.

The property is defined as a duration, e.g. ``60 seconds`` or ``100 millis``. By default it is ``10 minutes``.

.. _fsds_size_threshold_prop:

geomesa.fs.size.threshold
+++++++++++++++++++++++++

When specifying a target size for data files, this property controls the error margin that is considered acceptable.
Files which are outside of the margin may be merged or split during compactions. See :ref:`fsds_file_size_config`
for more information.

The threshold is specified as a float greater than ``0`` and less than ``1``, with a default value of ``0.05``.
For example, if the target file size is 100 bytes, then an error threshold of ``0.05`` means that files will not
be compacted if they are between 95 and 105 bytes.

geomesa.fs.validate.file
++++++++++++++++++++++++

When this property is set, files will be checked for data corruption after closing a file writer.

geomesa.fs.writer.partitions.max.open
+++++++++++++++++++++++++++++++++++++

When writing to multiple partitions, each partition writer is kept open until the overall feature writer is closed.
When writing to many partitions at once, this may cause memory problems due to the large number of writers. To
mitigate this, a limit may be set on the number of open partition writers.

Note that the data store parameter ``fs.writer.partitions.max.open`` will supersede this property.
