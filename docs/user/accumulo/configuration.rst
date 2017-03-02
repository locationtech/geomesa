Accumulo Configuration
======================

This section details Accumulo specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

Batch Writer Properties
-----------------------

The following properties control the configuration of Accumulo ``BatchWriter``\ s. They map directly to the
underlying ``BatchWriter`` methods.

geomesa.batchwriter.latency.millis
++++++++++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxLatency(long,%20java.util.concurrent.TimeUnit)>`__

geomesa.batchwriter.maxthreads
++++++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxWriteThreads(int)>`__

geomesa.batchwriter.memory
++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setMaxMemory(long)>`__

geomesa.batchwriter.timeout.millis
++++++++++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/client/BatchWriterConfig.html#setTimeout(long,%20java.util.concurrent.TimeUnit)>`__

Map Reduce Splits Properties
----------------------------

The following properties control the configure of Accumulo table splits.
See the `Accumulo Docs <https://accumulo.apache.org/1.7/accumulo_user_manual#_splitting>`__

geomesa.mapreduce.splits.max
++++++++++++++++++++++++++++

Set the absolute number of splits when configuring a mapper instead of allowing Accumulo to create a split
for each range or basing it on the number of tablet servers.

Setting this value overrides geomesa.mapreduce.splits.tserver.max.

geomesa.mapreduce.splits.tserver.max
++++++++++++++++++++++++++++++++++++

Set the max number of splits per tablet server when configuring a mapper. By default this value is
calculated using Accumulo's AbstractInputFormat.getSplits method which creates a split for each range. In
some scenarios this may create an undesirably large number of splits.

This value is overwritten by geomesa.mapreduce.splits.max if it is set.

Zookeeper Session Timeout
-------------------------

instance.zookeeper.timeout
++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/accumulo_user_manual.html#_instance_zookeeper_timeout>`__
