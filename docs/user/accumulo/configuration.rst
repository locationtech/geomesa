.. _accumulo_config_props:

Accumulo Configuration
======================

This section details Accumulo specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

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
