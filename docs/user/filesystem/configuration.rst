.. _fsds_config_props:

FileSystem DataStore Configuration
==================================

This section details configuration properties specific to the FileSystem data store. For general properties,
see :ref:`geomesa_site_xml`.

File Writers
------------

The following properties control the writing of data files.

geomesa.fs.writer.partition.timeout
+++++++++++++++++++++++++++++++++++

When writing to multiple partitions, each partition writer is kept open until the overall feature writer is closed.
When writing to many partitions at once, this may cause memory problems due to the large number of writers. To
mitigate this, idle partitions can be closed after a configurable timeout.

The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``.
