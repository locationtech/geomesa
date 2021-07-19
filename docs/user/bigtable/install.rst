Installing GeoMesa Bigtable
===========================

.. _install_bigtable_tools:

Installing the Binary Distribution
----------------------------------

GeoMesa Bigtable artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-bigtable_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-bigtable_${VERSION}-bin.tar.gz
    $ cd geomesa-bigtable_${VERSION}
    $ ls
    bin  conf  dist  docs  examples  lib  LICENSE.txt  logs

To run the command-line tools, the configuration file ``hbase-site.xml`` must be on the classpath. This can
be accomplished by placing the file in the ``conf`` folder. For more information, see `Connecting to Cloud Bigtable
<https://cloud.google.com/bigtable/docs/connecting-hbase>`__.

.. _install_bigtable_geoserver:

Installing GeoMesa Bigtable in GeoServer
----------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The HBase GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``dist/gs-plugins/target/geomesa-bigtable-gs-plugin_${VERSION}-install.tar.gz``
into GeoServer's ``WEB-INF/lib`` directory. This distribution does not include HBase or Hadoop JARs - the following JARs
should be copied into GeoServer's ``WEB-INF/lib`` directory. For convenience, the script ``bin/install-dependencies.sh``
will download them from Maven Central.

 * hbase-annotations-1.3.1.jar
 * hbase-client-1.3.1.jar
 * hbase-common-1.3.1.jar
 * hbase-hadoop2-compat-1.3.1.jar
 * hbase-hadoop-compat-1.3.1.jar
 * hbase-prefix-tree-1.3.1.jar
 * hbase-procedure-1.3.1.jar
 * hbase-protocol-1.3.1.jar
 * hbase-server-1.3.1.jar
 * commons-configuration-1.6.jar
 * hadoop-annotations-2.5.2.jar
 * hadoop-auth-2.5.2.jar
 * hadoop-common-2.5.2.jar
 * hadoop-hdfs-2.5.2.jar

(Note the versions may vary depending on your installation.)

The Bigtable data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). For more information, see `Connecting to Cloud Bigtable
<https://cloud.google.com/bigtable/docs/connecting-hbase>`__.

Restart GeoServer after the JARs are installed.
