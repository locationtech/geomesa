.. _hbase_config_props:

HBase Configuration
===================

This section details HBase specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

geomesa.hbase.client.scanner.caching.size
+++++++++++++++++++++++++++++++++++++++++

Set the number of rows that scanners will read ahead. If not set, the default caching will apply as configured in
``hbase-site.xml``. Higher caching values will enable faster scanners but will use more memory.

geomesa.hbase.config.paths
++++++++++++++++++++++++++

Additional configuration file paths, comma-delimited. The files will be added to the HBase configuration prior
to creating a ``Connection``. This property will be overridden by the data store configuration parameter,
if both are specified.

geomesa.hbase.coprocessor.arrow.enable
++++++++++++++++++++++++++++++++++++++

Disable coprocessor scans for Arrow queries, and use local encoding instead. This property will be overridden by
the data store configuration parameter, if both are specified.

geomesa.hbase.coprocessor.bin.enable
++++++++++++++++++++++++++++++++++++

Disable coprocessor scans for Bin queries, and use local encoding instead. This property will be overridden by
the data store configuration parameter, if both are specified.

geomesa.hbase.coprocessor.density.enable
++++++++++++++++++++++++++++++++++++++++

Disable coprocessor scans for density queries, and use local processing instead. This property will be overridden by
the data store configuration parameter, if both are specified.

geomesa.hbase.coprocessor.maximize.threads
++++++++++++++++++++++++++++++++++++++++++

Create a listener thread for each region when making coprocessor calls. If disabled, the number of listener threads
will be based on the data store configuration parameter ``hbase.coprocessor.threads``.

geomesa.hbase.coprocessor.url
+++++++++++++++++++++++++++++

Path to the GeoMesa jar containing coprocessors, for auto registration. This property will be overridden by
the data store configuration parameter, if both are specified.

geomesa.hbase.coprocessor.stats.enable
++++++++++++++++++++++++++++++++++++++

Disable coprocessor scans for stat queries, and use local processing instead. This property will be overridden by
the data store configuration parameter, if both are specified.

geomesa.hbase.coprocessor.yield.partial.results
+++++++++++++++++++++++++++++++++++++++++++++++

When true, this property has GeoMesa coprocessor calls yield and return to the client when the configured batch size
for that query is reached. When false, the coprocessor will attempt to complete its query (making multiple batches)
while respecting the ``geomesa.query.timeout``.

geomesa.hbase.remote.filtering
++++++++++++++++++++++++++++++

Disable remote filtering. Remote filtering and coprocessors speed up queries, however they require the installation
of custom JARs in HBase. Since this is not always possible, they can be disabled by setting this to ``false``.
This property will be overridden by the data store configuration parameter, if both are specified.

geomesa.hbase.scan.buffer
+++++++++++++++++++++++++

Specify the maximum number of results to pre-buffer in local memory when executing a scan, if the client is not
consuming the results as fast as they are being returned.

geomesa.hbase.table.availability.timeout
++++++++++++++++++++++++++++++++++++++++

Specify the amount of time to wait for a table to become available after it has been created. The timeout
is specified as a duration, e.g. ``5 minutes``.

geomesa.hbase.wal.durability
++++++++++++++++++++++++++++

Set the client side WAL (write ahead log) durability setting. This can improve performance when running large
ingests where performance is of more concern than reliability. Available settings are:

- ASYNC_WAL: Write the Mutation to the WAL asynchronously
- FSYNC_WAL: Write the Mutation to the WAL synchronously and force the entries to disk.
- SKIP_WAL: Do not write the Mutation to the WAL
- SYNC_WAL: Write the Mutation to the WAL synchronously.
- USE_DEFAULT: If this is for tables durability, use HBase's global default value (SYNC_WAL).

For addtional information see `HBase documentation
<https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Durability.html>`__.

geomesa.hbase.write.batch
+++++++++++++++++++++++++

Specify the number of bytes that will be buffered before flushing to disk during write operations.

geomesa.hbase.query.block.caching.enabled
+++++++++++++++++++++++++++++++++++++++++

Set whether blocks should be cached for scans, true by default. When true, default settings of the table and
family are used (this will never override caching blocks if the block cache is disabled for that family or entirely).
