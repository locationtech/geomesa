Upgrade Guide
=============

This section describes code and configuration changes required when upgrading GeoMesa versions.

Version 1.4
+++++++++++

Data Store Parameters
---------------------

In version 1.4, GeoMesa has standardized the parameters used in calls to ``DataStoreFinder`` and the Spark
``SpatialRDDProvider``. New parameters are outlined in the individual data store pages:

  * :ref:`accumulo_parameters`
  * :ref:`hbase_parameters`
  * :ref:`bigtable_parameters`
  * :ref:`cassandra_parameters`
  * :ref:`kafka_parameters`
  * :ref:`lambda_parameters`

System Properties
-----------------

Time-related system properties have been standardized to all use readable durations. Durations can be specified
as a number followed by a time unit, e.g. ``10 minutes`` or ``30 seconds``. The following properties
have been changed to accept durations, and some have been renamed. More details can be found under
:ref:`geomesa_site_xml` or the appropriate data store configuration section.

==================================== ===========================================
Property                             Previous name
==================================== ===========================================
geomesa.query.timeout                geomesa.query.timeout.millis
geomesa.metadata.expiry              N/A
geomesa.batchwriter.latency          geomesa.batchwriter.latency.millis
geomesa.batchwriter.latency          geomesa.batchwriter.latency.millis
geomesa.stats.compact.interval       geomesa.stats.compact.millis
geomesa.cassandra.read.timeout       geomesa.cassandra.read.timeout.millis
geomesa.cassandra.connection.timeout geomesa.cassandra.connection.timeout.millis
==================================== ===========================================

Kafka Data Store
----------------

The Kafka Data Store has been rewritten into a single implementation for Kafka 0.9 and 0.10. Support for
Kafka 0.8 has been removed. See :ref:`kafka_index` for more information.
