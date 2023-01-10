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
`DataStore <https://docs.geotools.org/stable/javadocs/org/geotools/api/data/DataStore.html>`__ level. In the future,
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
Version 5.0.0 Upgrade Guide
=======
=======
>>>>>>> 52032d25f6e (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 44834baf12c (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 41b0b79cc0d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 17e4255218e (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 7db4865b988 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6c4eb6353a7 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 3a8f16e50e (Add note on NiFi scala version to upgrade guide)
>>>>>>> 71febf7318d (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 83363fd6e61 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9761fd9aca5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b34b2cac86 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 71329efd07f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 25eb151d8cd (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> c71bcc193a2 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 839a9b3e57d (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> a84b894f501 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d174f455090 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a2249a4f921 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 1412fafd4f5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bd947b7816e (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> ba806c25f9a (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
=======
=======
>>>>>>> 40423f41e64 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 21338adb2fa (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6aa4affaabf (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 8c0a0c82122 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 9166a90e23 (Add note on NiFi scala version to upgrade guide)
>>>>>>> d027f78786a (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3a8f16e50e (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 3bb01897ac (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb24731debc (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> d027f78786a (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 40423f41e64 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 83363fd6e61 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9761fd9aca5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 21338adb2fa (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6aa4affaabf (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 4b34b2cac86 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71329efd07f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 8c0a0c82122 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d381f46e90 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 25eb151d8cd (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 6eee7c559a (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> c71bcc193a2 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> b3209273eb (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 839a9b3e57d (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a84b894f501 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> d174f455090 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 07107f12d3 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> a2249a4f921 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1412fafd4f5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 364909bdaf (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> bd947b7816e (Add note on NiFi scala version to upgrade guide)
=======
=======
=======
>>>>>>> 3a8f16e50e (Add note on NiFi scala version to upgrade guide)
>>>>>>> aaa8412afd (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> ba806c25f9a (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 9166a90e23 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> d027f78786a (Add note on NiFi scala version to upgrade guide)
=======
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 40423f41e64 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 21338adb2fa (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 6aa4affaabf (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 8c0a0c82122 (GEOMESA-3254 Add Bloop build support)
Version 4.1.0 Upgrade Guide
>>>>>>> 00f021f6c00 (Add note on NiFi scala version to upgrade guide)
+++++++++++++++++++++++++++

Version Compatibility
---------------------

GeoMesa 5.0.x is generally compatible with versions 4.0.x and 3.5.x across different environments. This means that
it is possible to upgrade in parts; i.e. upgrade GeoServer to 5.0.0 but keep NiFi at 3.5.2. Please note that
previously deprecated functionality (see below) may no longer work once any part of the environment is upgraded to
5.0.0.

Java Version
------------

GeoMesa no longer supports Java 8. The Java ecosystem is slowly moving on from Java 8, and it is no longer
possible to support Java 8 while staying up-to-date with dependencies and security patches. GeoMesa now
supports Java versions 11 and 17.

GeoTools Upgrade
----------------

GeoTools has been updated to version 30.2. This version contains
`extensive package changes <https://geotoolsnews.blogspot.com/2023/10/geotools-300-released.html>`__. In addition to
the Ant migration script provided by GeoTools, GeoMesa provides a ``sed``
`script <https://github.com/locationtech/geomesa/blob/geomesa-5.0.0/build/gt-30-api-changes.sed>`__ to help migrate Scala projects.
Use the following ``bash`` script to upgrade a project from an earlier version of GeoTools to version 30:

.. code::

    # run the following from the root directory of your project:
    wget 'https://raw.githubusercontent.com/locationtech/geomesa/geomesa-5.0.0/build/remove-opengis.xml'
    ant -f remove-opengis.xml
    # for Scala projects, also run:
    wget 'https://raw.githubusercontent.com/locationtech/geomesa/geomesa-5.0.0/build/gt-30-api-changes.sed'
    find . -name "*.scala" -not -exec grep -q "import org.geotools.api.data._" {} \; -exec sed -E -i -f gt-30-api-changes.sed {} \;

HBase Versions
--------------

Support for HBase 1.4 has been dropped, as HBase 1.4 does not support Java 11.

Dependency Version Upgrades
---------------------------

The following dependencies have been upgraded:

* accumulo ``2.0.1`` -> ``2.1.2``
* aircompressor ``0.21`` -> ``0.25``
* antlr ``4.7.1`` -> ``4.7.2``
* arrow ``11.0.0`` -> ``15.0.2``
* avro ``1.11.1`` -> ``1.11.3``
* aws-java-sdk ``1.11.179`` -> ``1.12.625``
* caffeine ``2.9.3`` -> ``3.1.8``
* cassandra-driver ``3.11.3`` -> ``3.11.5``
* com.clearspring.analytics ``2.9.2`` -> ``2.9.8``
* com.fasterxml.jackson ``2.14.1`` -> ``2.16.1``
* com.jayway.jsonpath ``2.7.0`` -> ``2.9.0``
* com.typesafe:config ``1.4.2`` -> ``1.4.3``
* commons-cli ``1.2`` -> ``1.6.0``
* commons-codec ``1.15`` -> ``1.16.0``
* commons-compress ``1.22`` -> ``1.26.0``
* commons-configuration2 ``2.5`` -> ``2.10.1``
* commons-csv ``1.9.0`` -> ``1.10.0``
* commons-dbcp2 ``2.6.0`` -> ``2.11.0``
* commons-io ``2.8.0`` -> ``2.15.1``
* commons-lang3 ``3.8.1`` -> ``3.14.0``
* commons-pool2 ``2.6.1`` -> ``2.12.0``
* commons-text ``1.10.0`` -> ``1.11.0``
* confluent ``6.2.7`` -> ``6.2.9``
* cqengine ``3.0.0`` -> ``3.6.0``
* org.apache.curator ``4.3.0`` -> ``5.6.0``
* geotools ``28.2`` -> ``30.2``
* gson ``2.10`` -> ``2.10.1``
* guava ``30.1-jre`` -> ``32.0.0-jre``
* hadoop ``2.10.2`` -> ``3.3.6``
* hbase ``2.5.2`` -> ``2.5.8-hadoop3``
* httpclient ``4.5.13`` -> ``4.5.14``
* httpcore ``4.4.15`` -> ``4.4.16``
* io.netty ``4.1.85.Final`` -> ``4.1.106.Final``
* javax.measure:unit-api ``2.0`` -> ``2.1.2``
* jcommander ``1.78`` -> ``1.82``
* jedis ``4.3.1`` -> ``5.1.0``
* kafka ``2.8.2`` -> ``3.7.0``
* kryo ``4.0.2`` -> ``4.0.3``
* orc ``1.8.2`` -> ``1.9.3``
* org.eclipse.emf.common ``2.15.0`` -> ``2.29.0``
* org.eclipse.emf.ecore ``2.15.0`` -> ``2.35.0``
* org.eclipse.emf.ecore.xmi ``2.15.0`` -> ``2.36.0``
* org.ehcache:sizeof ``0.4.0`` -> ``0.4.3``
* parquet ``1.12.3`` -> ``1.13.1``
* parboiled ``1.3.1`` -> ``1.4.1``
* postgresql ``42.5.1`` -> ``42.7.2``
* pureconfig ``0.17.2`` -> ``0.17.4``
* saxon ``11.4`` -> ``12.4``
* scala ``2.12.17`` -> ``2.12.18``
* scala-parser-combinators ``2.1.1`` -> ``2.3.0``
* scala-xml ``2.1.0`` -> ``2.2.0``
* sedona ``1.3.1-incubating`` -> ``1.5.0``
* si.uom ``2.0.1`` -> ``2.1``
* spark ``3.3.1`` -> ``3.5.0``
* spring-security ``5.8.0`` -> ``5.8.11``
* systems.uom ``2.0.2`` -> ``2.1``
* tech.units:indriya ``2.0.2`` -> ``2.2``
* tech.uom.lib ``2.0`` -> ``2.1``
* xmlresolver ``4.4.3`` -> ``5.2.2``
* zookeeper ``3.5.10`` -> ``3.9.2``

Deprecated Classes and Methods
------------------------------

The following classes have been deprecated and will be removed in a future version:

* org.locationtech.geomesa.kafka.confluent.SchemaParser.GeoMesaAvroDeserializableEnumProperty

<<<<<<< HEAD
<<<<<<< HEAD
Removed Modules
---------------

The ``geomesa-metrics-ganglia`` module has been removed, and Ganglia is no longer supported as a destination for
metrics.

As part of dropping support for HBase 1.x, the ``geomesa-hbase-distributed-runtime-hbase1``,
``geomesa-hbase-server-hbase1`` and ``geomesa-hbase-spark-runtime-hbase1`` modules have been removed.

GeoMesa Convert OSM
-------------------

The ``geomesa-convert-osm`` module has been relocated to https://github.com/geomesa/geomesa-convert-osm.

=======
<<<<<<< HEAD
>>>>>>> 1d14c6bb326 (Add note on NiFi scala version to upgrade guide)
=======
<<<<<<< HEAD
>>>>>>> 98a051b4665 (Add note on NiFi scala version to upgrade guide)
Partitioned PostGIS Prepared Statements
---------------------------------------

If not specified, prepared statements now default to ``true``  in the partitioned PostGIS data store. Prepared
statements are generally faster on insert, and some attribute types (such as list-type attributes) are only
supported through prepared statements.

=======
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2dd5db0392 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 01f791d4aa (Add note on NiFi scala version to upgrade guide)
=======
<<<<<<< HEAD
=======
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
>>>>>>> 3bb01897ac (Add note on NiFi scala version to upgrade guide)
=======
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2dd5db0392 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 01f791d4aa (Add note on NiFi scala version to upgrade guide)
>>>>>>> d381f46e90 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
>>>>>>> b3209273eb (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2dd5db0392 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 07107f12d3 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 01f791d4aa (Add note on NiFi scala version to upgrade guide)
>>>>>>> 364909bdaf (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 3bb01897ac (Add note on NiFi scala version to upgrade guide)
>>>>>>> 9166a90e23 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
Version 4.0.0 Upgrade Guide
+++++++++++++++++++++++++++

<<<<<<< HEAD
Version Compatibility
---------------------

GeoMesa 4.0.0 is focused on upgrading dependency versions and removing deprecated features, and only contains
a few new features. To make upgrading easier, version 4.0.x is generally compatible with version 3.5.x across
different environments. This means that it is possible to upgrade in parts; i.e. upgrade GeoServer to 4.0.0
but keep NiFi at 3.5.1. Please note that previously deprecated functionality (see below) may no longer work once
any part of the environment is upgraded to 4.0.0.

=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
Version 4.0.0 Upgrade Guide
+++++++++++++++++++++++++++

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
Scala Versions
--------------

Scala 2.11 support has been removed, and Scala 2.13 support has been added (in addition to the existing
Scala 2.12 support).

GeoTools/GeoServer Versions
---------------------------

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
GeoTools has been upgraded from ``23.3`` to ``28.2``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.2``.
=======
GeoTools has been upgraded from ``23.3`` to ``28.0``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.0``.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
GeoTools has been upgraded from ``23.3`` to ``28.2``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.2``.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
GeoTools has been upgraded from ``23.3`` to ``28.2``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.2``.
=======
GeoTools has been upgraded from ``23.3`` to ``28.0``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.0``.
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
GeoTools has been upgraded from ``23.3`` to ``28.2``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.2``.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
GeoTools has been upgraded from ``23.3`` to ``28.2``. GeoServer has been upgrade from ``2.17.3`` to ``2.22.2``.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
JTS has been upgraded from ``1.17.0`` to ``1.19.0``.

As part of this upgrade, various GeoTools methods have changed in incompatible ways. Several classes, in
particular ``Query`` and ``SimpleFeatureBuilder``, have changed from accepting arrays to using varargs (variable
arguments). Additionally, the various ``DataStore`` methods, such as ``DataStoreFinder``, now require
``Map<String, ?>`` instead of ``Map<String, ? extends Serializable>``.

Dependency Version Upgrades
---------------------------

<<<<<<< HEAD
The following high-level dependencies have been upgraded:
=======
The following high-level dependencies have been upgraded. For a full changelist of all the dependency changes,
see TODO commit hash link on github
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)

* Apache Hadoop ``2.8.5`` -> ``2.10.2``
* Apache Spark ``2.4.7`` -> ``3.3.1``
* Apache Accumulo ``2.0.0`` -> ``2.0.1``
<<<<<<< HEAD
* Apache HBase ``1.4.12`` -> ``1.4.14``, ``2.2.3`` -> ``2.5.2``
* Apache Kafka ``2.1.1`` -> ``2.8.2``
* Apache Arrow ``0.16.0`` -> ``11.0.0``
<<<<<<< HEAD
* Apache Avro ``1.8.2`` -> ``1.11.1``
* Apache Parquet ``1.9.0`` -> ``1.12.3``
* Apache Orc ``1.5.4`` -> ``1.8.2``
=======
* Apache HBase ``1.4.12`` -> ``1.4.13``, ``2.2.3`` -> ``2.4.4``
* Apache Kafka ``2.1.1`` -> ``2.8.2``
* Apache Arrow ``0.16.0`` -> ``10.0.1``
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 52579ffb37 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52579ffb37 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 6c49bcd685 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
<<<<<<< HEAD
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a928f2f739 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 630900cfbb (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 52579ffb37 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
* Apache Avro ``1.8.2`` -> ``1.11.1``
* Apache Parquet ``1.9.0`` -> ``1.12.3``
* Apache Orc ``1.5.4`` -> ``1.8.1``
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> a928f2f73 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
>>>>>>> 05a1868e90 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d18777a94f (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 320759d5d3 (GEOMESA-3246 Upgrade Arrow to 11.0.0)
=======
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
* Apache Avro ``1.8.2`` -> ``1.11.1``
* Apache Parquet ``1.9.0`` -> ``1.12.3``
* Apache Orc ``1.5.4`` -> ``1.8.1``
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
* Jedis ``3.0.1`` -> ``4.3.1``
* Confluent ``5.1.0`` -> ``6.2.7``
* Kryo ``3.0.3`` -> ``4.0.2``
* Typesafe Config ``1.3.3`` -> ``1.4.2``
* EJML ``0.34`` -> ``0.41``
<<<<<<< HEAD
* Saxon ``9.7.0-20`` -> ``11.4``

For a full changelist of all dependencies, see the diff
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1931ca9b2 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> ccf4d7c3bd (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
=======
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> f01f17feca (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> d1931ca9b2 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
`here <https://gist.github.com/elahrvivaz/f86d31f78b57bf92113c16661a886c12/revisions?diff=split>`__.

Minimum Library Versions
------------------------

Support for older versions of some libraries has been dropped. The following minimum versions are now required:

* Apache Accumulo ``2.0.0`` (dropped support for ``1.7``, ``1.8``, ``1.9``, and ``1.10``)
* Apache Spark ``3.0`` (dropped support for ``2.4``)
* Apache Kafka ``2.0`` (dropped support for ``0.10``, ``0.11``, ``1.0``, and ``1.1``)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dc6c9b2b22 (Add min library version to upgrade guide)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 51960ee4cb (Add min library version to upgrade guide)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> dc6c9b2b22 (Add min library version to upgrade guide)
>>>>>>> 4ceb5035c3 (Add min library version to upgrade guide)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
`here <https://gist.github.com/elahrvivaz/dd76a6c03154f9c65ce596e965a9c084/revisions?diff=split>`__.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> d1931ca9b2 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
`here <https://gist.github.com/elahrvivaz/f86d31f78b57bf92113c16661a886c12/revisions?diff=split>`__.
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> f813a10b55 (Add min library version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ceb5035c3 (Add min library version to upgrade guide)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
`here <https://gist.github.com/elahrvivaz/f86d31f78b57bf92113c16661a886c12/revisions?diff=split>`__.
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
>>>>>>> ccf4d7c3bd (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc6c9b2b22 (Add min library version to upgrade guide)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
`here <https://gist.github.com/elahrvivaz/f86d31f78b57bf92113c16661a886c12/revisions?diff=split>`__.
>>>>>>> 27d2a13b23 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
>>>>>>> f01f17feca (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
>>>>>>> 51960ee4cb (Add min library version to upgrade guide)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> d1931ca9b2 (GEOMESA-3246 Update Scala to 2.12.17 (#2976))
=======
=======
>>>>>>> dc6c9b2b22 (Add min library version to upgrade guide)
>>>>>>> 4ceb5035c3 (Add min library version to upgrade guide)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)

Removal of Deprecated Modules
-----------------------------

The following deprecated modules were removed:

* geomesa-bigtable
* geomesa-kudu
* geomesa-stream
* geomesa-geojson
* geomesa-web
* geomesa-feature-nio
* geomesa-convert-metrics-cloudwatch
* geomesa-convert-metrics-ganglia
* geomesa-convert-metrics-graphite

In addition, various other deprecated classes and methods were removed. To identify any code that requires changes,
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
build your project against GeoMesa 3.5.1 and note any deprecation warnings generated by the compiler.
=======
build your project against GeoMesa 3.5.0 and note any deprecation warnings generated by the compiler.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
build your project against GeoMesa 3.5.1 and note any deprecation warnings generated by the compiler.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
build your project against GeoMesa 3.5.1 and note any deprecation warnings generated by the compiler.
=======
build your project against GeoMesa 3.5.0 and note any deprecation warnings generated by the compiler.
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
build your project against GeoMesa 3.5.1 and note any deprecation warnings generated by the compiler.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> 54b98b2eea (GEOMESA-3253 Include full dependency change list for 4.0.0)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
build your project against GeoMesa 3.5.1 and note any deprecation warnings generated by the compiler.
>>>>>>> fa3c19722d (GEOMESA-3253 Include full dependency change list for 4.0.0)
>>>>>>> 717bb1864b (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> dd6626657f (GEOMESA-3253 Include full dependency change list for 4.0.0)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)

Package Changes
---------------

The following packages were moved, renamed or split in order to support Java 11 modules:

* ``org.locationtech.geomesa.jobs.accumulo`` -> ``org.locationtech.geomesa.accumulo.jobs``
* ``org.locationtech.geomesa.spark.accumulo`` -> ``org.locationtech.geomesa.accumulo.spark``
* ``org.locationtech.geomesa.spark.hbase`` -> ``org.locationtech.geomesa.hbase.spark``
* ``org.locationtech.geomesa.arrow.vector`` (partial) -> ``package org.locationtech.geomesa.arrow.jts``
* ``org.locationtech.geomesa.parquet`` -> ``org.locationtech.geomesa.fs.storage.parquet``
* ``org.locationtech.geomesa.process`` (partial) -> ``org.locationtech.geomesa.process.wps``

<<<<<<< HEAD
GeoMesa NiFi Changes
--------------------

GeoMesa NiFi is now built against NiFi 1.19.1. The GeoMesa NARs and JARs have been renamed to include the Scala
version (i.e. ``geomesa-datastore-services-nar_2.12-4.0.0.nar``). The datastore-specific processors (e.g.
``PutGeoMesaHBase``) have been removed in favor of the generic processors (e.g. ``PutGeoMesa``). The
recommended upgrade path is to first upgrade to GeoMesa NiFi 3.5.1, and replace all the datastore-specific
processors in the flow. This will ensure that the flow is still valid after upgrading to GeoMesa NiFi 4.0.0.
The ``geomesa-accumulo2-nar`` has been replaced with ``geomesa-accumulo20-nar``, and there is an additional
``geomesa-accumulo21-nar`` for Accumulo 2.1 support.

=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
Scan Range Changes
------------------

GeoMesa will now generate a more accurate number of ranges based on ``geomesa.scan.ranges.target``. Users
who have configured this property should verify their setting is still appropriate, especially if set to a
large value. Setting ``geomesa.scan.ranges.recurse`` to ``7`` will restore the old behavior if needed.

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c0571b43e1 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7d4ed7605c (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7d4ed7605c (GEOMESA-3262 Postgis - add config to skip whole world filters)
>>>>>>> 75d5a347f8 (GEOMESA-3262 Postgis - add config to skip whole world filters)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7d4ed7605c (GEOMESA-3262 Postgis - add config to skip whole world filters)
>>>>>>> b5c0452b13 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c0571b43e1 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
Partitioned PostGIS Query Changes
---------------------------------

GeoMesa will now ignore queries that encompass the entire world in the partitioned PostGIS data store. For more
information, refer to :ref:`postgis_filter_world`.

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75d5a347f8 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> b5c0452b13 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 75d5a347f8 (GEOMESA-3262 Postgis - add config to skip whole world filters)
>>>>>>> c0571b43e1 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7d4ed7605c (GEOMESA-3262 Postgis - add config to skip whole world filters)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c0571b43e1 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 75d5a347f8 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b5c0452b13 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 75d5a347f8 (GEOMESA-3262 Postgis - add config to skip whole world filters)
>>>>>>> c0571b43e1 (GEOMESA-3262 Postgis - add config to skip whole world filters)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
Version 3.5.0 Upgrade Guide
+++++++++++++++++++++++++++

Removal of Log4j
----------------

GeoMesa has been updated to ban all usages of ``log4j``, to mitigate various CVEs present in that framework. In
most cases, GeoMesa uses ``slf4j``, and delegates to the logging framework of the runtime environment.
However, this change impacts the JARs bundled with the command-line tools, which now ship with
`reload4j <https://reload4j.qos.ch/>`__ instead. Other environments using GeoMesa (i.e. GeoServer) must be
hardened independently.

Kafka Serialization
-------------------

The GeoMesa Kafka data store now supports a new serialization format, ``avro-native``. This format uses Avro
array and map types for ``List`` and ``Map`` type attributes, which makes it easier to read with standard Avro
tools. Note that GeoMesa versions before 3.5.0 will not be able to consume topics written in this format.

Deprecated Modules
------------------

The following modules have been deprecated, and will be removed in a future version:

* GeoMesa Bigtable

Dependency Updates
------------------

* org.slf4j:slf4j-api: ``1.7.25`` -> ``1.7.36``
* com.google.code.gson:gson: ``2.8.1`` -> ``2.8.9``

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d381f46e90 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 07107f12d3 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 364909bdaf (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> aaa8412afd (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e6dd9b5b1d (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 2dd5db0392 (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 01f791d4aa (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> e6dd9b5b1d (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 3a8f16e50e (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
>>>>>>> 3bb01897ac (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> d381f46e90 (Add note on NiFi scala version to upgrade guide)
=======
=======
<<<<<<< HEAD
>>>>>>> e6dd9b5b1d (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 6eee7c559a (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
>>>>>>> b3209273eb (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 07107f12d3 (Add note on NiFi scala version to upgrade guide)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 364909bdaf (Add note on NiFi scala version to upgrade guide)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> e6dd9b5b1d (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> 3a8f16e50e (Add note on NiFi scala version to upgrade guide)
<<<<<<< HEAD
>>>>>>> aaa8412afd (Add note on NiFi scala version to upgrade guide)
=======
=======
=======
>>>>>>> e6dd9b5b1 (Add note on NiFi scala version to upgrade guide)
>>>>>>> 16e5072a4a (Add note on NiFi scala version to upgrade guide)
>>>>>>> 3bb01897ac (Add note on NiFi scala version to upgrade guide)
>>>>>>> 9166a90e23 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5a0b3f2c1d (Add note on NiFi scala version to upgrade guide)
>>>>>>> 49f1028075 (Add note on NiFi scala version to upgrade guide)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
Version 3.3.0 Upgrade Guide
+++++++++++++++++++++++++++

Scala Versions
--------------

GeoMesa NiFi NARs now ship with Scala 2.12 by default. This should be largely transparent to end-users, however
any custom GeoMesa converter JARs used in NiFi and written in Scala will need to be compiled with Scala 2.12.

Version 3.2.0 Upgrade Guide
+++++++++++++++++++++++++++

Scala Versions
--------------

GeoMesa now supports Scala 2.12. Scala 2.11 support has been deprecated and will be removed in a future version.

Spark Versions
--------------

GeoMesa now supports Spark 3.0 and 3.1. Support for Spark 2.3 and 2.4 has been deprecated and will be removed
in a future version.

Dependency Updates
------------------

* com.fasterxml.jackson: ``2.9.10`` -> ``2.12.1``

FileSystem Data Store Metadata Format Change
--------------------------------------------

The metadata format for the FileSystem data store has been changed to support storing arbitrary key-value pairs.
Any data written with version 3.2.0 or later will not be readable by earlier GeoMesa versions.

Lambda Data Store Binary Distribution Change
--------------------------------------------

The Lambda data store binary distribution no longer contains the ``geomesa-accumulo-distributed-runtime`` JAR.
This JAR is available in the Accumulo data store binary distribution.

StrategyDecider API Update
--------------------------

The ``org.locationtech.geomesa.index.planning.StrategyDecider`` API has been extended with an optional
``GeoMesaStats`` argument that enables stat-based strategy decisions. The old API method has been deprecated
and will be removed in a future version.

Deprecated Modules
------------------

The following modules have been deprecated, and will be removed in a future version:

* GeoMesa Kudu
* GeoMesa Streaming (Camel integration)
* GeoMesa Web
* GeoMesa GeoJSON

Deprecated Arrow Output Options
-------------------------------

The Arrow output options for providing cached dictionaries, returning multiple logical files, and running
queries in two passes have been deprecated and will be removed in the next major version.

Version 3.1.0 Upgrade Guide
+++++++++++++++++++++++++++

Maven Type of GeoServer Plugin Modules
--------------------------------------

All of the ``geomesa-*-gs-plugin`` artifacts have been changed to ``<type>pom</type>``, since they did not
contain any code. Any ``pom.xml`` references to them should be updated to use the correct type.

Avro Version Update
-------------------

The version of Avro used by GeoMesa has been updated from 1.7.5 to 1.8.2. Avro serialized files should
be compatible between versions, but compile and runtime dependencies may need to be updated if a project
uses Avro and references GeoMesa.

Query Interceptors API Change
-----------------------------

The query interceptors API has been expanded to support query guards. Any existing query interceptor
implementations will continue to work, but may need to be re-compiled against the GeoMesa 3.1.0.

Dependency Updates
------------------

* GeoTools: ``23.0`` -> ``23.3``
* Avro: ``1.7.5`` -> ``1.8.2``

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

The GeoMesa NiFi processors have been updated to NiFi 11 and split out into separate ``nar`` files for each
supported back-end database. Additionally, there are separate ``nar`` files for HBase 1.4/2.2 and Accumulo 1.9/2.0,
respectively. The processor classes and configurations have also changed. See :ref:`nifi_bundle` for details.

Dependency Updates
------------------

* Apache Arrow: ``0.10`` -> ``0.16``

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
per-feature, as described in :ref:`data_security`.

Version 2.4.0 Upgrade Guide
+++++++++++++++++++++++++++

GeoTools 21 and GeoServer 2.15
------------------------------

GeoMesa 2.4.0 is compiled against GeoTools 21.1 and GeoServer 2.15. This version of GeoTools contains package
and class location changes to support Java 11. Due to the changes, GeoMesa will no longer work with older
versions of GeoTools and GeoServer.

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
been standardized. New parameters are outlined in the individual data store pages:

  * :ref:`accumulo_parameters`
  * :ref:`hbase_parameters`
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

The Kafka Data Store has been rewritten into a single implementation for all supported Kafka versions. Support for
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
