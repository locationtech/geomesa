Installing GeoMesa Bigtable
===========================

.. _bigtable_install_source:

Building from Source
--------------------

GeoMesa BigTable must be built from source. For more information refer
to :ref:`building_from_source` in the developer manual, or to the
``README.md`` file in the the source distribution. Build using using the
``bigtable`` Maven profile:

.. code-block:: bash

    $ mvn clean install -Pbigtable

The Bigtable-specific code will be found in the ``geomesa-hbase/geomesa-bigtable-*``
directories of the source distribution.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _install_bigtable_geoserver:

Installing GeoMesa Bigtable in GeoServer
----------------------------------------

The Bigtable GeoServer plugin is not bundled by default in the GeoMesa binary distribution
and should be built from source.

After building, extract ``geomesa-hbase/geomesa-bigtable-gs-plugin/target/geomesa-bigtable-gs-plugin_2.11-$VERSION-install.tar.gz``
into GeoServer's ``WEB-INF/lib`` directory. This distribution does not include HBase or Hadoop JARs; the following JARs
should be copied into GeoServer's ``WEB-INF/lib`` directory:

 * commons-configuration-1.6.jar
 * hadoop-annotations-2.5.2.jar
 * hadoop-auth-2.5.2.jar
 * hadoop-client-2.5.2.jar
 * hadoop-common-2.5.2.jar
 * hadoop-hdfs-2.5.2.jar
 * hbase-annotations-1.1.2.jar
 * hbase-client-1.2.3.jar
 * hbase-common-1.2.3.jar
 * hbase-hadoop2-compat-1.1.2.jar
 * hbase-hadoop-compat-1.1.2.jar
 * hbase-prefix-tree-1.1.2.jar
 * hbase-procedure-1.1.2.jar
 * hbase-protocol-1.2.3.jar
 * hbase-server-1.1.2.jar

(Note the versions may vary depending on your installation.)

The Bigtable data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). For more information, see `Connecting to Cloud Bigtable
<https://cloud.google.com/bigtable/docs/connecting-hbase>`__.

Restart GeoServer after the JARs are installed.

Jackson Version
^^^^^^^^^^^^^^^

.. warning::

    Some GeoMesa functions (in particular Arrow conversion) requires ``jackson-core-2.6.x``. Some versions
    of GeoServer ship with an older version, ``jackson-core-2.5.0.jar``. After installing the GeoMesa
    GeoServer plugin, be sure to delete the older JAR from GeoServer's ``WEB-INF/lib`` folder.
