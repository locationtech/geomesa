.. _kudu_config_props:

Kudu Data Store Configuration
=============================

This section details Kudu-specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

geomesa.kudu.admin.timeout
++++++++++++++++++++++++++

Sets the default timeout used for Kudu administrative operations (create tables, delete tables, etc).
Should be set as a duration, e.g. ``30 seconds``, ``1 minute``. If not specified, the default
value is 30 seconds. Use ``0 seconds`` to disable the timeout.

geomesa.kudu.operation.timeout
++++++++++++++++++++++++++++++

Sets the default timeout used for Kudu user operations (using sessions and scanners). Should be set
as a duration, e.g. ``30 seconds``, ``1 minute``. If not specified, the default value is 30 seconds. Use
``0 seconds`` to disable the timeout.

geomesa.kudu.socket.timeout
+++++++++++++++++++++++++++

Sets the default timeout to use when waiting on data from a socket. Should be set as a duration,
e.g. ``30 seconds``, ``1 minute``. If not specified, the default value is 10 seconds. Use ``0 seconds``
to disable the timeout.

geomesa.kudu.block.size
+++++++++++++++++++++++

Sets the block size for Kudu columns, in bytes. From the Kudu documentation:

.. pull-quote::

  This is the number of bytes of user data packed per block on disk, and represents the unit of IO when
  reading this column. Larger values may improve scan performance, particularly on spinning media. Smaller
  values may improve random access performance, particularly for workloads that have high cache hit rates
  or operate on fast storage such as SSD.

  Note that the block size specified here corresponds to uncompressed data. The actual size of the unit read
  from disk may be smaller if compression is enabled.

  It's recommended that this not be set any lower than 4096 (4KB) or higher than 1048576 (1MB).

geomesa.kudu.compression
++++++++++++++++++++++++

Sets the default compression used for Kudu columns. May be one of ``NO_COMPRESSION``, ``SNAPPY``, ``LZ4``, or
``ZLIB``. If not specified, the default is ``LZ4``. This may also be overridden on a per-column basis; see
:ref:`kudu_column_compression`.

More information is available in the `Kudu documentation <http://kudu.apache.org/docs/schema_design.html#compression>`__.

geomesa.kudu.mutation.buffer
++++++++++++++++++++++++++++

Sets the number of write operations that will be buffered before flushing to disk. If not specified,
the default is 10000.
