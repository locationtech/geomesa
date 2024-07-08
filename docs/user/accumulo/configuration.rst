.. _accumulo_config_props:

Accumulo Configuration
======================

This section details Accumulo specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

General Properties
------------------

geomesa.accumulo.table.cache.expiry
+++++++++++++++++++++++++++++++++++

The expiry to cache the existence of tables, defined as a duration, e.g. ``60 seconds`` or ``100 millis``. To avoid frequent
checks for the existence of tables before writing, tables checks are cached. If tables are deleted without stopping any ingest,
they will not be re-created until the cache expires.

Default is ``10 minutes``.

geomesa.accumulo.table.sync
+++++++++++++++++++++++++++

Sets the level of synchronization when creating and deleting tables. When using tables backed by S3, synchronization
may prevent table corruption errors in Accumulo. Possible values are:

* ``zookeeper`` (default) - uses a distributed lock that works across JVMs.
* ``local`` - uses an in-memory lock that works within a single JVM.
* ``none`` - does not use any external locking. Generally this is safe when using tables backed by HDFS.

The synchronization level may be adjusted depending on the architecture being used - for example, if tables are created
by a single-thread, then a system may safely disable synchronization.

Batch Writer Properties
-----------------------

The following properties control the configuration of Accumulo ``BatchWriter``\ s. They map directly to the
underlying ``BatchWriter`` methods.

geomesa.batchwriter.latency
+++++++++++++++++++++++++++

The latency is defined as a duration, e.g. ``60 seconds`` or ``100 millis``. See the `Accumulo API`__ for details.

__ https://accumulo.apache.org/1.9/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxLatency(long,%20java.util.concurrent.TimeUnit)

geomesa.batchwriter.maxthreads
++++++++++++++++++++++++++++++

Determines the max threads used for writing. See the `Accumulo API`__ for details.

__ https://accumulo.apache.org/1.9/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxWriteThreads(int)

geomesa.batchwriter.memory
++++++++++++++++++++++++++

The memory is defined in bytes, e.g. ``10mb`` or ``100kb``. See the `Accumulo API`__ for details.

__ https://accumulo.apache.org/1.9/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxMemory(long)

geomesa.batchwriter.timeout.millis
++++++++++++++++++++++++++++++++++

The timeout is defined as a duration, e.g. ``60 seconds`` or ``100 millis``. See the `Accumulo API`__ for details.

__ https://accumulo.apache.org/1.9/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setTimeout(long,%20java.util.concurrent.TimeUnit)

Remote Processing Properties
----------------------------

The following properties control the push-down processing for certain queries to the Accumulo tablet servers, vs
processing in the client. Enabling push-down processing can result in faster queries, but also puts additional
load on the Accumulo cluster, which may negatively impact concurrent clients.

See also `ref:accumulo_parameters` for configuring these properties directly in the data store parameters.

geomesa.accumulo.remote.arrow.enable
++++++++++++++++++++++++++++++++++++

Enable processing Arrow encoding in Accumulo tablets servers as a distributed call, instead of encoding
locally in the client. Default is ``true``.

geomesa.accumulo.remote.bin.enable
++++++++++++++++++++++++++++++++++

Enable processing binary encoding in Accumulo tablets servers as a distributed call, instead of encoding
locally in the client. Default is ``true``.

geomesa.accumulo.remote.density.enable
++++++++++++++++++++++++++++++++++++++

Enable processing heatmap encoding in Accumulo tablets servers as a distributed call, instead of encoding
locally in the client. Default is ``true``.

geomesa.accumulo.remote.stats.enable
++++++++++++++++++++++++++++++++++++

Enable processing statistical calculations in Accumulo tablets servers as a distributed call, instead of
encoding locally in the client. Default is ``true``.

Map Reduce Input Splits Properties
----------------------------------

The following properties control the number of input splits for a map reduce job. See the
`Accumulo User Manual`__ for details.

__ https://accumulo.apache.org/1.9/accumulo_user_manual#_splitting

geomesa.mapreduce.splits.max
++++++++++++++++++++++++++++

Set the absolute number of splits when configuring a mapper instead of allowing Accumulo to create a split
for each range or basing it on the number of tablet servers.

Setting this value overrides ``geomesa.mapreduce.splits.tserver.max``.

geomesa.mapreduce.splits.tserver.max
++++++++++++++++++++++++++++++++++++

Set the max number of splits per tablet server when configuring a mapper. By default this value is
calculated using Accumulo's ``AbstractInputFormat.getSplits`` method which creates a split for each range. In
some scenarios this may create an undesirably large number of splits.

This value is overwritten by ``geomesa.mapreduce.splits.max`` if it is set.

Zookeeper Session Timeout
-------------------------

instance.zookeeper.timeout
++++++++++++++++++++++++++

The Zookeeper timeout is defined in milliseconds, according to the Accumulo specification. See the
`Accumulo User Manual`__ for details.

__ https://accumulo.apache.org/1.9/accumulo_user_manual.html#_instance_zookeeper_timeout
