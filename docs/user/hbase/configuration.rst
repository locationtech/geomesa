.. _hbase_config_props:

HBase Configuration
===================

This section details HBase specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

geomesa.hbase.config.paths
++++++++++++++++++++++++++

Additional configuration file paths, comma-delimited. The files will be added to the HBase configuration prior
to creating a ``Connection``. This property will be overridden by the data store configuration parameter,
if both are specified.

geomesa.hbase.remote.filtering
++++++++++++++++++++++++++++++

Disable remote filtering. Remote filtering and coprocessors speed up queries, however they require the installation
of custom JARs in HBase. Since this is not always possible, they can be disabled by setting this to ``false``.
This property will be overridden by the data store configuration parameter, if both are specified.

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

geomesa.hbase.client.scanner.caching.size
+++++++++++++++++++++++++++++++++++++++++

Set the number of rows that scanners will read ahead. If not set, the default caching will apply as configured in
``hbase-site.xml``. Higher caching values will enable faster scanners but will use more memory.

geomesa.hbase.query.block.caching.enabled
+++++++++++++++++++++++++++++++++++++++++

Set whether blocks should be cached for scans, true by default. When true, default settings of the table and
family are used (this will never override caching blocks if the block cache is disabled for that family or entirely).
