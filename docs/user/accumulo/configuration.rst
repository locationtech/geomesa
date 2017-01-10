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

Zookeeper Session Timeout
-------------------------

instance.zookeeper.timeout
++++++++++++++++++++++++++

See the `Accumulo API <https://accumulo.apache.org/1.7/accumulo_user_manual.html#_instance_zookeeper_timeout>`__
