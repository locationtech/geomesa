.. _fsds_config_props:

FileSystem DataStore Configuration
==================================

System Properties
-----------------

This section details configuration properties specific to the FileSystem data store. For general properties,
see :ref:`geomesa_site_xml`.

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

This property is implemented only for Parquet files. If set, it checks a file for any potential data corruption
upon closing the file writer.

geomesa.fs.writer.partition.timeout
+++++++++++++++++++++++++++++++++++

When writing to multiple partitions, each partition writer is kept open until the overall feature writer is closed.
When writing to many partitions at once, this may cause memory problems due to the large number of writers. To
mitigate this, idle partitions can be closed after a configurable timeout.

The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``.

Hadoop Configuration Properties
-------------------------------

Some properties are configured through Hadoop's ``hdfs-site.xml``. Additional configuration files can be passed in through the
:ref:`fsds_parameters` ``fs.config.paths`` and ``fs.config.xml``.

geomesa.fs.visibilities
+++++++++++++++++++++++

This property can be used to skip writing visibility labels, by setting it to ``false``. If the data being written is known to
not have any labels, the visibility column can be removed. See :ref:`data_security` for an overview of security labels.

geomesa.parquet.bounding-boxes
++++++++++++++++++++++++++++++

This property can be used to skip writing bounding boxes for geometry-type columns when using Parquet, by setting
it to ``false``. By default, each geometry will include an array-type column that includes the minimum and maximum extents
of the geometry. This can be used to accelerate queries through push-down filtering.

.. _fsds_parquet_geometries_prop:

geomesa.parquet.geometries
++++++++++++++++++++++++++

This property can be used to control the encoding schema used for geometry-type columns when using Parquet. The available options
are:

* ``GeoParquetWkb`` (default) - This schema uses `GeoParquet 1.1.0 <https://geoparquet.org/releases/v1.1.0/>`__ with geometries
  encoded as WKB. This format is supported by most 3rd party libraries that can read GeoParquet.
* ``GeoParquetNative`` - This schema uses `GeoParquet 1.1.0 <https://geoparquet.org/releases/v1.1.0/>`__ with geometries
  encoded "natively". This format doesn't require special libraries to read, but isn't as widely supported as WKB.
* ``GeoMesaV1`` (deprecated) - This schema is similar to ``GeoParquetNative``, but is not an official specification that other
  libraries can read. It may be useful if interoperability is required with older versions of GeoMesa (prior to 5.3.0),
  as those versions are not able to read GeoParquet files.
* ``GeoMesaV0`` (deprecated) - This schema is similar to ``GeoMesaV1``, but only supports point-type geometries and has other
  minor inconsistencies.
