.. _fsds_config_props:

FileSystem DataStore Configuration
==================================

This section details configuration properties specific to the FileSystem data store. For general properties,
see :ref:`geomesa_site_xml`.

File Writers
------------

The following properties control the writing of data files.

.. _fsds_size_threshold_prop:


geomesa.fs.validate.file
+++++++++++++++++++++++++

This property is implemented only for Parquet files. If set, it checks a file for any potential data corruption
upon closing the file writer.

geomesa.fs.size.threshold
+++++++++++++++++++++++++

When specifying a target size for data files, this property controls the error margin that is considered acceptable.
Files which are outside of the margin may be merged or split during compactions. See :ref:`fsds_file_size_config`
for more information.

The threshold is specified as a float greater than ``0`` and less than ``1``, with a default value of ``0.05``.
For example, if the target file size is 100 bytes, then an error threshold of ``0.05`` means that files will not
be compacted if they are between 95 and 105 bytes.


geomesa.fs.writer.partition.timeout
+++++++++++++++++++++++++++++++++++

When writing to multiple partitions, each partition writer is kept open until the overall feature writer is closed.
When writing to many partitions at once, this may cause memory problems due to the large number of writers. To
mitigate this, idle partitions can be closed after a configurable timeout.

The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``.

FileSystem Operations
---------------------

geomesa.fs.file.cache.duration
++++++++++++++++++++++++++++++

To avoid repeated reads from disk, GeoMesa will cache the results of disk operations for a certain period of time.
This has the side effect that files modified by external processes may not be visible until after the cache timeout.

The property is defined as a duration, e.g. ``60 seconds`` or ``100 millis``. By default it is ``10 minutes``.
