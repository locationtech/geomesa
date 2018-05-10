.. _upgrade_guide:

Upgrade Guide
=============

This section contains general information on version upgrades, as well as version-specific changes that may
affect the end user.

Compatibility Across Versions
+++++++++++++++++++++++++++++

Semantic Versioning
-------------------

Starting with 2.0.0, GeoMesa is trying to adhere to `semantic versioning <https://semver.org/>`__. Essentially,
releases are broken down into major, minor and patch versions. For a version number like 2.0.1, 2 is the major
version, 2.0 is the minor version, and 2.0.1 is the patch version.

Major version updates contain breaking client API changes. Minor version updates contain new or updated functionality
that is backwards-compatible. Patch versions contain only backwards-compatible bug fixes. This delineation allows
users to gauge the potential impact of updating versions.

.. warning::

  Versions prior to 2.0.0 do not follow semantic versioning, and each release should be
  considered a major version change.

Compatibility
-------------

Semantic versioning only makes guarantees about the public API of a project, however the GeoMesa public API is not
currently well defined. In addition, GeoMesa has several other compatibility vectors to consider:

Data Compatibility
^^^^^^^^^^^^^^^^^^

Data compatibility refers to the ability to read and write data written with older versions of GeoMesa. GeoMesa
fully supports data written with version 1.2.2 or later, and mostly supports data written with 1.1.0 or later.

Note that although later versions can read earlier data, the reverse is not necessarily true. Data written
with a newer client may not be readable by an older client.

Data written with 1.2.1 or earlier can be migrated to a newer data format. See :ref:`index_upgrades` for details.
Note that this functionality is currently only implemented for Accumulo.

Distributed Runtime Compatibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For data stores with a distributed component (e.g. Accumulo or HBase), distributed runtime compatibility refers
to the ability to write or query data with a client that is a different version than the distributed code.
Similarly, this also covers the ability to have different versions of the distributed code on different machines in
a single cluster.

GeoMesa currently requires that all client and server JARs are the same minor version.

Dependency Compatibility
^^^^^^^^^^^^^^^^^^^^^^^^

Dependency compatibility refers to the ability to update GeoMesa without updating other components
(e.g. Accumulo, HBase, Hadoop, Spark, GeoServer, etc). Generally, GeoMesa supports a range of dependency versions
(e.g. Accumulo 1.6 to 1.9). Spark versions are more tightly coupled, due to the use of private Spark APIs.

Compatibility Matrix
--------------------

+---------------------+-------+-------+-------+
|                     | Major | Minor | Patch |
+=====================+=======+=======+=======+
| Data                | Y     | Y     | Y     |
+---------------------+-------+-------+-------+
| Distributed runtime | N     | N     | Y     |
+---------------------+-------+-------+-------+
| Dependencies        | N     | N     | Y     |
+---------------------+-------+-------+-------+

Version 2.0.0 Upgrade Guide
+++++++++++++++++++++++++++

GeoTools 18 and GeoServer 2.12
------------------------------

GeoMesa 2.0.0 is compiled against GeoTools 18.0 and GeoServer 2.12. When upgrading GeoServer instances,
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

Saxon XML Parser
----------------

The GeoMesa converter XML module now ships with Saxon-HE by default. Saxon-HE is generally much faster
at parsing XML than the default Java implementation. Previously, Saxon was available as an additional download.

.. warning::

  Saxon parsing has some differences from the default Java implementation, which may cause existing
  converter definitions to fail. In particular, Saxon is much stricter with XML namespaces. See
  :ref:`xml_converter_namespaces` for more information.

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
