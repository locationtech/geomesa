.. _upgrade_guide:

Upgrade Guide
=============

This section contains general information on version upgrades, as well as version-specific changes that may
affect the end user.

Compatibility Across Versions
+++++++++++++++++++++++++++++

Semantic Versioning
-------------------

Starting with 2.0.0, GeoMesa is adhering to `semantic versioning <https://semver.org/>`__. Essentially,
releases are broken down into major, minor and patch versions. For a version number like 2.0.1, 2 is the major
version, 2.0 is the minor version, and 2.0.1 is the patch version.

Major version updates contain breaking public API changes. Minor version updates contain new or updated functionality
that is backwards-compatible. Patch versions contain only backwards-compatible bug fixes. This delineation allows
users to gauge the potential impact of updating versions.

.. warning::

  Versions prior to 2.0.0 do not follow semantic versioning, and each release should be
  considered a major version change.

Compatibility
-------------

Semantic versioning makes API guarantees, but GeoMesa has several compatibility vectors to consider:

Data Compatibility
^^^^^^^^^^^^^^^^^^

Data compatibility refers to the ability to read and write data written with older versions of GeoMesa. GeoMesa
fully supports data written with version 1.2.2 or later, and mostly supports data written with 1.1.0 or later.

Note that although later versions can read earlier data, the reverse is not necessarily true. Data written
with a newer client may not be readable by an older client.

Data written with 1.2.1 or earlier can be migrated to a newer data format. See :ref:`index_upgrades` for details
(note that this functionality is currently only implemented for Accumulo).

API Compatibility
^^^^^^^^^^^^^^^^^

The GeoMesa public API is not currently well defined, so API compatibility is only guaranteed at the GeoTools
`DataStore <http://docs.geotools.org/stable/javadocs/org/geotools/data/DataStore.html>`__ level. In the future,
GeoMesa will clearly indicate which classes and methods are part of the public API. Non-public classes may change
without warning between minor versions.

Binary Compatibility
^^^^^^^^^^^^^^^^^^^^

Binary compatibility refers to the ability to have different GeoMesa versions in a single environment. An environment
may be a single process or span multiple servers (for example an ingest pipeline, a query client, and an analytics
platform). For data stores with a distributed component (HBase and Accumulo), the environment includes both the
client and the distributed code.

GeoMesa requires that all JARs in an environment are the same minor version, and that all JARs within a single JVM
are the same patch version.

Dependency Compatibility
^^^^^^^^^^^^^^^^^^^^^^^^

Dependency compatibility refers to the ability to update GeoMesa without updating other components
(e.g. Accumulo, HBase, Hadoop, Spark, GeoServer, etc). Generally, GeoMesa supports a range of dependency versions
(e.g. Accumulo 1.6 to 1.9). Spark versions are more tightly coupled, due to the use of private Spark APIs.

Pre-Release Code
^^^^^^^^^^^^^^^^

GeoMesa sometimes provides modules in an alpha or beta state. Although they share the overall GeoMesa version number,
such modules should be considered pre-1.0, and are not guaranteed to provide any forwards or backwards compatibility
across versions. Pre-release modules will be clearly marked in the documentation.

Compatibility Matrix
--------------------

+--------------+-------+-------+-------+
|              | Major | Minor | Patch |
+==============+=======+=======+=======+
| Data         | Y     | Y     | Y     |
+--------------+-------+-------+-------+
| API          | N     | Y     | Y     |
+--------------+-------+-------+-------+
| Binary       | N     | N     | Y     |
+--------------+-------+-------+-------+
| Dependencies | N     | N     | Y     |
+--------------+-------+-------+-------+

Version 3.0.0 Upgrade Guide
+++++++++++++++++++++++++++

Removal of Deprecated Modules and Classes
-----------------------------------------

GeoMesa 3.0.0 removes several lesser-used modules, as well as various obsolete classes and methods.

The modules removed are: ``geomesa-accumulo/geomesa-accumulo-compute``,
``geomesa-accumulo/geomesa-accumulo-native-api``, ``geomesa-accumulo/geomesa-accumulo-raster-distributed-runtime``,
``geomesa-accumulo/geomesa-accumulo-raster``, ``geomesa-accumulo/geomesa-accumulo-security``,
``geomesa-accumulo/geomesa-accumulo-stats-gs-plugin``, ``geomesa-convert/geomesa-convert-scripting``,
``geomesa-convert/geomesa-convert-simplefeature``, ``geomesa-hbase/geomesa-hbase-native-api``,
``geomesa-metrics``, ``geomesa-native-api``, ``geomesa-spark/geomesa-spark-geotools``, ``geomesa-blobstore/*``, and
``geomesa-web/geomesa-web-data``.

The classes and methods removed are detailed in `GEOMESA-2284 <https://geomesa.atlassian.net/browse/GEOMESA-2284>`_.

HBase 2 Support
---------------

GeoMesa 3.0.0 supports both HBase 1.4 and HBase 2.2. HBase 1.3 is no longer supported. HBase 2.0 and 2.1 are
not officially supported, but may work in some cases.

There are now two separate modules for HBase filters and coprocessors - ``geomesa-hbase-distributed-runtime-hbase1``
and ``geomesa-hbase-distributed-runtime-hbase2``. The previous ``geomesa-hbase-distributed-runtime`` module has
been removed. Users should install the distributed runtime corresponding to their HBase installation.

Similarly, there are now two separate modules for HBase Spark support - ``geomesa-hbase-spark-runtime-hbase1`` and
``geomesa-hbase-spark-runtime-hbase2``. The previous ``geomesa-hbase-spark-runtime`` module has been removed.
Users should use the Spark runtime corresponding to their HBase installation.

Accumulo 2 Support
------------------

GeoMesa 3.0.0 supports both Accumulo 1.9 with Hadoop 2.8 and Accumulo 2.0 with Hadoop 3.
Earlier versions of Accumulo are no longer supported, but may work in some cases.

There are now two separate modules for Accumulo Spark support - ``geomesa-accumulo-spark-runtime-accumulo1`` and
``geomesa-accumulo-spark-runtime-accumulo2``. The previous ``geomesa-accumulo-spark-runtime`` module has been removed.
Users should use the Spark runtime corresponding to their Accumulo installation.

NiFi Processors
---------------

The GeoMesa NiFi processors have been split out into separate nar files for each supported back-end database.
Additionally, there are separate nar files for HBase 1.4/2.2 and Accumulo 1.9/2.0, respectively. The processor
classes and configurations have also changed. See :ref:`nifi_bundle` for details.

Dependency Updates
------------------

* Apache Arrow: 0.10 -> 0.16

Apache Arrow Updates
--------------------

As part of the upgrade to Apache Arrow 0.16, the geomesa-arrow modules have been refactored to simplify memory
management and allocation. Some classes have been removed, and some interfaces have changed. This may impact
anyone using the geomesa-arrow modules directly.

The Arrow IPC format changed in Arrow 0.15. Older clients may not be able to read Arrow-encoded results by
default. To enabled the 'legacy' Arrow IPC format, set the system property ``geomesa.arrow.format.version``
to ``0.10``, or use the query hint ``ARROW_FORMAT_VERSION``. See :ref:`arrow_encoding` for details.

Converter Date Functions
------------------------

The converter functions ``isoDate`` and ``isoDateTime`` have been updated to match the equivalent Java
``DateTimeFormatter`` pattern. ``isoDate`` has changed from ``yyyyMMdd`` to ``yyyy-MM-dd``, while ``isoDateTime``
has changed from ``yyyyMMdd'T'HHmmss.SSSZ`` to ``yyyy-MM-dd'T'HH:mm:ss``. The old patterns can still be
referenced through ``basicDate`` and ``basicDateTime``.

AuthorizationsProvider and AuditProvider API Change
---------------------------------------------------

The signature for ``org.locationtech.geomesa.security.AuthorizationsProvider#configure`` and
``org.locationtech.geomesa.utils.audit.AuditProvider#configure`` have changed slightly from
``void configure(Map<String, Serializable> params)`` to
``public void configure(Map<String, ? extends Serializable> params)``. Any classes implementing either of these
interfaces will need to update their method signature. Any classes invoking these methods should not need to updated,
as the new signature is compatible with the old one.

Accumulo Default Visibilities Removed
-------------------------------------

The Accumulo data store parameter ``geomesa.security.visibilities`` have been removed. Visibilities should be set
per-feature, as per :ref:`accumulo_visibilities`.

Version 2.4.0 Upgrade Guide
+++++++++++++++++++++++++++

GeoTools 21 and GeoServer 2.15
------------------------------

GeoMesa 2.4.0 is compiled against GeoTools 21.1 and GeoServer 2.15. This version of GeoTools contains package
and class location changes to support Java 11. Due to the changes, GeoMesa will no longer work with older
versions of GeoTools and GeoServer.

.. warning::

  GeoMesa 2.4.0 requires GeoTools 21.x and GeoServer 2.15.x.

Configuration of Cached Statistics
----------------------------------

GeoMesa 2.4.0 moves the configuration of cached stats from a data store parameter (where it has to be set every time)
to the feature type user data (where it is set once at schema creation, and only changed through explicit schema
updates). See :ref:`stat_config` for more details.

Feature types that were created in prior versions will continue to behave as before, with the configuration
determined by the data store parameter each time. The configuration can be set permanently through
the ``updateSchema`` data store method or the :ref:`cli_update_schema` CLI command.

Indexing of Timestamp Attributes
--------------------------------

GeoMesa 2.4.0 fully supports indexing of ``java.sql.Timestamp`` attributes. In previous versions, timestamp
attribute indices were not officially supported, however they did work in some cases. Any data that was written to
a timestamp attribute index with an older version will no longer be readable by GeoMesa 2.4.0. To migrate old
data, **truncate the index table** first, then re-write all existing records:

.. code-block:: scala

    import org.geotools.data.{DataStoreFinder, Query, Transaction}
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
    import org.locationtech.geomesa.utils.geotools.FeatureUtils

    val params: java.util.Map[String, String] = ??? // data store connection parameters
    val ds: GeoMesaDataStore[_] = DataStoreFinder.getDataStore(params).asInstanceOf[GeoMesaDataStore[_]]
    val typeName: String = ??? // simple feature type name to update
    val timestamps: Seq[String] = ??? // names of any timestamp-type attributes
    val indices = ds.manager.indices(ds.getSchema(typeName)).filter(_.attributes.headOption.exists(timestamps.contains))
    val writer = ds.getIndexWriterAppend(typeName, indices)
    val features = ds.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)
    try {
      while (features.hasNext) {
        FeatureUtils.write(writer, features.next(), useProvidedFid = true)
      }
    } finally {
      features.close()
      writer.close()
    }

NiFi Processor Changes
----------------------

The GeoMesa NiFi processors have been refactored to support NiFi nar inheritance and as a first step towards supporting
Java 11. Any existing processors will continue to work under the older version, as long as you don't delete the old
GeoMesa nar file. However, you will need to create new processors in order to upgrade to 2.4.0.

Distribution of Installation Bundles
------------------------------------

As of GeoMesa 2.4.0, installation bundles (binary distribution and GeoServer plugin tar files) will no
longer be hosted on Maven Central. They will continue to be available on
`GitHub <https://github.com/locationtech/geomesa/releases>`__ and the
`Locationtech Maven Repository <https://repo.eclipse.org/content/groups/releases>`__. Note that this only
applies to large installation bundles; GeoMesa will continue to publish JAR files to Maven Central.

HBase GeoServer Plugin Installation
-----------------------------------

The GeoMesa HBase GeoServer plugin installation tar file has been updated to remove the shaded HBase client JARs.
The appropriate client JARS for your HBase version now must be installed separately. See
:ref:`install_hbase_geoserver` for details.

If desired, the shaded GeoMesa JAR is still available from Maven, as
``org.locationtech.geomesa:geomesa-hbase-gs-plugin_2.11`` with the classifier ``shaded``. However, this will likely
be removed in the next major version release.

Version 2.3.0 Upgrade Guide
+++++++++++++++++++++++++++

Default Query Planning Type
---------------------------

GeoMesa 2.3.0 changes the default query planning type from stat-based to heuristic-based. This will only affect the
Accumulo data store, as other stores have not implemented statistics. To enable stat-based query planning, refer
to :ref:`query_planning_hint`.

Immutable Simple Feature Types
------------------------------

GeoMesa 2.3.0 returns immutable objects from calls to ``getSchema``. This allows for the re-use of SimpleFeatureType
instances, which reduces overhead. In most cases, this will have no effect on end users, however note that mutable
and immutable feature types will never be ``equals`` when compared directly.

In order to update a schema, or if mutability is desired for some other reason, call
``org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.mutable()`` to create a mutable copy. Java users
can call ``org.locationtech.geomesa.utils.interop.SimpleFeatureTypes.mutable()`` instead.

FileSystem Storage API Changes
------------------------------

The FileSystem Storage API is still considered beta-level software, and has been updated in this release. The
DataStore API has not changed, however the internal class interfaces have changed in this release, potentially
requiring changes in user code.

In addition, the format used to store metadata files has been updated, so older versions of GeoMesa will not be
able to read metadata created with this version.

Deprecated Modules
------------------

The following modules have been deprecated, and will be removed in a future version:

* GeoMesa Raster
* GeoMesa Native API
* GeoMesa Blob Store
* GeoMesa Metrics

Version 2.2.0 Upgrade Guide
+++++++++++++++++++++++++++

GeoTools 20 and GeoServer 2.14
------------------------------

GeoMesa 2.2.0 is compiled against GeoTools 20.0 and GeoServer 2.14. This version of GeoTools upgrades JTS
from 1.14 to 1.16, which includes a transition of the project to Locationtech. The new version
of JTS renames the packages from ``com.vividsolutions`` to ``org.locationtech.jts``. Due to the package renaming,
GeoMesa will no longer work with older versions of GeoTools and GeoServer.

.. warning::

  GeoMesa 2.2.0 requires GeoTools 20.x and GeoServer 2.14.x.

Accumulo DataStore GeoServer Installation
-----------------------------------------

When using GeoServer, the GeoMesa Accumulo data store now requires Accumulo client JARs 1.9.2 or later.
This is due to classpath conflicts between earlier Accumulo clients and GeoServer 2.14. Fortunately, newer Accumulo
clients can talk to older Accumulo instances, so it is only necessary to upgrade the client JARs in GeoServer,
but not the entire Accumulo cluster.

Version 2.1.0 Upgrade Guide
+++++++++++++++++++++++++++

Converter Updates
-----------------

The GeoMesa converter API has been updated and simplified. The old API has been deprecated, and while custom
converters written against it should still work, users are encouraged to migrate to
``org.locationtech.geomesa.convert2.SimpleFeatureConverter``. A compatibility bridge is provided so that
all converters registered with either the new or old API will be available to both.

Converter definitions should continue to work the same, but some invalid definitions may start to fail due to
stricter configuration parsing.

XML Converter Namespaces
^^^^^^^^^^^^^^^^^^^^^^^^

XML parsing is now namespace-aware. This shouldn't affect most operations, but any custom converter functions
that operate on the XML element objects may need to take this into account (for example, custom XPath querying).

Distributed Runtime Version Checks
----------------------------------

To prevent unexpected bugs due to JAR version mismatches, GeoMesa can scan the distributed classpath to
verify compatible versions on the distributed classpath. This behavior may be enabled by setting the system
property ``geomesa.distributed.version.check=true``.

Shapefile Ingestion
-------------------

Shapefile ingestion through the GeoMesa command-line tools has changed to use a converter definition. This allows
for on-the-fly modifications to the shapefile during ingestion, however the command now requires user confirmation.
The previous behavior can be simulated by passing ``--force`` to the ingest command.

Delimited Text Auto-Ingestion
-----------------------------

GeoMesa previously supported auto ingest of specially formatted delimited CSV and TSV files. This functionality
has been replaced with standard ingest type inference, which works similarly but may create different results.
Generally, the previous behavior can be replicated by using type inference to create a converter definition,
then modifying the converter to set the feature ID to the first column (``$1``).

FileSystem Storage API Changes
------------------------------

The FileSystem Storage API is still considered beta-level software, and has been updated in this release. The
DataStore API has not changed, however the internal class interfaces have changed in this release, potentially
requiring changes in user code.

In addition, the format used to store metadata files has been updated, so older versions of GeoMesa will not be
able to read metadata created with this version. When accessing older metadata for the first time, GeoMesa will
update the files to the new format, potentially breaking any old clients still being used.

Finally, the ``update-metadata`` tools command has been replaced with ``manage-metadata``.

Spark Version Update
--------------------

GeoMesa now builds against Spark 2.3.1, and supports versions 2.2.x and 2.3.x.

Arrow Version Update
--------------------

The version of Apache Arrow used for Arrow-encoded results has been updated from 0.6.0 to 0.10.0. Due to changes
in the Arrow inter-process communication (IPC) format, clients may need to update to the same Arrow version.

Scalatra Version Update
-----------------------

The version of scalatra used for web servlets has been updated to 2.6.3. The new version requires json4s 3.5.4,
which may require changes to the web server used to deploy the servlets.

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
