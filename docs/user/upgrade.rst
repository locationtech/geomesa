.. _upgrade_guide:

Upgrade Guide
=============

This section describes code and configuration changes required when upgrading GeoMesa versions.

Version 1.4
+++++++++++

GeoTools 18 and GeoServer 2.12
------------------------------

GeoMesa 1.4 is compiled against GeoTools 18.0 and GeoServer 2.12. When upgrading GeoServer instances,
it's usually best to start over with a new GeoServer data directory. If you upgrade GeoMesa in an existing
GeoServer instance that has run GeoMesa 1.3.x or earlier, layers will still work but you will not be
able to edit any existing GeoMesa stores. In order to edit stores, you will need to delete them and
re-create them through the GeoServer UI. Alternatively, you may edit the GeoServer ``datastore.xml`` files
(located in the GeoServer data directory) to match the new GeoMesa data store parameters (described below).
In particular, you will need to add a ``namespace`` parameter that matches the workspace of the GeoServer store.

Data Store Parameters
---------------------

The data store parameters used in calls to ``DataStoreFinder`` and the Spark ``SpatialRDDProvider`` have
been standardized . New parameters are outlined in the individual data store pages:

  * :ref:`accumulo_parameters`
  * :ref:`hbase_parameters`
  * :ref:`bigtable_parameters`
  * :ref:`cassandra_parameters`
  * :ref:`kafka_parameters`
  * :ref:`lambda_parameters`

The older parameter names will continue to work, but are deprecated and may be removed in future versions.

Removal of Joda Time
--------------------

With the introduction of ``java.time`` in Java 8, the Joda Time project has been deprecated. As such, GeoMesa
has removed its Joda dependency in favor of ``java.time``. One consequence of this is that custom date patterns
in ``geomesa-convert`` are interpreted slightly differently. See `DateTimeFormatter`__ for details.

__ https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

.. warning::

  In particular, "year of era" has changed from ``Y`` to ``y``. ``Y`` now means "week-based year", and will
  give different results.

Kafka Data Store
----------------

The Kafka Data Store has been rewritten into a single implementation for Kafka |kafka_version|. Support for
Kafka 0.8 has been removed. See :ref:`kafka_index` for more information.

Accumulo Standardization
------------------------

In order to standardize behavior between data store implementations, some behaviors of the ``AccumuloDataStore``
have been modified.

Attribute Index Coverage
^^^^^^^^^^^^^^^^^^^^^^^^

Accumulo attribute indices specified with ``index=true`` will now create full attribute indices, instead of
join indices. To create a join index, explicitly specify ``index=join``. Existing schemas are not affected.

Record Index Identifier
^^^^^^^^^^^^^^^^^^^^^^^

The Accumulo ``record`` index has been renamed to the ``id`` index. In general practice, this will have no effect,
however when specifying ``geomesa.indices.enabled``, the value ``id`` must be used in place of ``records``.

Tools Command Name
^^^^^^^^^^^^^^^^^^

The Accumulo command line tools script has been renamed from ``geomesa`` to ``geomesa-accumulo``.

Table Splitters
---------------

The table splitting API has changed. Any custom table splitters implementing
``org.locationtech.geomesa.index.conf.TableSplitter`` will need to be updated for the new method signatures.
In addition, the provided GeoMesa splitters have been deprecated and replaced. See :ref:`table_split_config`
for more details.

System Properties
-----------------

Time-related system properties have been standardized to all use readable durations. Durations can be specified
as a number followed by a time unit, e.g. ``10 minutes`` or ``30 seconds``. The following properties
have been changed to accept durations, and some have been renamed. Note that this will affect system properties
set in the JVM as well as any custom ``geomesa-site.xml`` files. More details can be found under
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
