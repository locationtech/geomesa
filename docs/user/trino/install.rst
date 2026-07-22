.. _trino_install:

Installing GeoMesa Trino
========================

Building from Source
--------------------

The build runs Maven on **JDK 17**; only ``geomesa-trino-plugin``'s ``javac`` is forked to a
**JDK 25** toolchain (Trino 481 is Java 25 bytecode) — the data store builds on JDK 17.
Both modules are part of the default reactor. Register the toolchain once (idempotent):

.. code-block:: bash

    build/scripts/update-maven-toolchains.sh   # writes the JDK 17 + 25 entries to ~/.m2/toolchains.xml

Then, from the repository root:

.. code-block:: bash

    mvn clean install -pl geomesa-trino/geomesa-trino-plugin,geomesa-trino/geomesa-trino-datastore -am -DskipTests
    # unit tests (no Trino required)
    mvn test -pl geomesa-trino/geomesa-trino-plugin,geomesa-trino/geomesa-trino-datastore

The plugin fat JAR is produced at
``geomesa-trino/geomesa-trino-plugin/target/geomesa-trino-plugin-{{release}}.jar``
(skip the ``original-*`` shade input next to it). It bundles all dependencies except:

* ``trino-spi`` — provided by Trino's SPI parent classloader
* ``log4j-slf4j-impl`` / ``log4j-slf4j2-impl`` / ``logback`` — excluded to avoid a circular
  SLF4J↔Log4j bridge that ``Log4jLoggerFactory`` rejects at class init time
* the unused Iceberg catalog / filesystem backends pulled in by ``trino-iceberg`` — Snowflake
  (the JDBC driver alone is ~230 MB), Azure, GCS, and Alluxio — pruned via ``<exclusions>`` since
  the supported deployments use REST/Glue catalogs + S3. This trims the jar from ~360 MB to
  ~159 MB. ``iceberg-aws``, ``trino-filesystem-s3``, and ``trino-hive`` (+ Coral/Calcite, which
  ``trino-hive`` needs at runtime on the Glue path) are kept.

Deploying the Plugin into a Trino Cluster
-----------------------------------------

**1. Install the plugin.** Trino loads each subdirectory of its plugin directory
(``/usr/lib/trino/plugin/`` in the standard layout) as an independent plugin. Create a
new subdirectory on every coordinator and worker and copy the fat JAR into it:

.. code-block:: bash

    mkdir /usr/lib/trino/plugin/spatial-iceberg
    cp geomesa-trino-plugin-{{release}}.jar /usr/lib/trino/plugin/spatial-iceberg/

The directory name is arbitrary — anything that doesn't collide with the built-in
``iceberg`` plugin.

**2. Define a catalog** backed by the connector, e.g.
``/etc/trino/catalog/spatial_iceberg.properties``. Set ``connector.name=spatial_iceberg``
plus the stock Iceberg connector's configuration — the connector passes Iceberg
properties through to its delegate. REST-catalog example:

.. code-block:: properties

    connector.name=spatial_iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://<catalog-host>:8181
    iceberg.file-format=PARQUET
    fs.s3.enabled=true
    s3.endpoint=<s3-endpoint>
    s3.region=<region>
    s3.aws-access-key=<key>
    s3.aws-secret-key=<secret>

Glue example:

.. code-block:: properties

    connector.name=spatial_iceberg
    iceberg.catalog.type=glue
    iceberg.file-format=PARQUET
    iceberg.metadata-cache.enabled=true
    hive.metastore.glue.region=<region>
    hive.metastore.glue.default-warehouse-dir=s3://<bucket>/<warehouse-prefix>
    fs.s3.enabled=true
    s3.region=<region>

The connector passes any non-``geomesa.*`` property straight through to the Iceberg
delegate. For the GeoMesa-specific catalog properties — including
``geomesa.spatial.bbox-page-filter`` and the ``geomesa.security.*`` row-visibility
keys — see :ref:`trino_configuration`.

**3. Restart Trino** on all nodes. Verify the catalog is up with ``SHOW CATALOGS;``,
then confirm pruning is active with the EXPLAIN checks under :ref:`trino_verify_pruning`.

A stock-Iceberg baseline catalog (``connector.name=iceberg``) pointed at the same
metastore and warehouse is useful for side-by-side comparison, but do not expose it
to untrusted users if you rely on Trino-layer row visibility — only the
``spatial_iceberg`` catalog enforces it.

Installing the Data Store
-------------------------

``geomesa-trino-datastore`` is a client-side library: put it (plus the matching
``trino-jdbc`` driver) on the application classpath. See :ref:`trino_parameters` for
connection parameters and programmatic access.
