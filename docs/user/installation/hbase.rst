Installing GeoMesa HBase
========================

Installing from the Binary Distribution
---------------------------------------

GeoMesa HBase artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version (``$VERSION`` = |release|)
and untar it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution
    $ wget http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-hbase-dist_2.11/$VERSION/geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz
    $ tar xvf geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-hbase-dist_2.11-$VERSION
    $ ls
    bin/  conf/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _hbase_install_source:

Building from Source
--------------------

GeoMesa HBase may also be built from source. For more information refer to :ref:`building_from_source`
in the :doc:`/developer/index`, or to the ``README.md`` file in the the
source distribution. The remainder of the instructions in this chapter assume
the use of the binary GeoMesa HBase distribution. If you have built from source, the
distribution is created in the ``target`` directory of
``geomesa-hbase/geomesa-hbase-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_hbase_commandline:

Setting up the HBase Command Line Tools
---------------------------------------

GeoMesa HBase comes with a set of command line tools for managing HBase features located in ``geomesa-accumulo_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in geomesa-accumulo_2.11-$VERSION/bin/geomesa-env.sh.

In the ``geomesa-accumulo_2.11-$VERSION`` directory, run ``bin/geomesa configure`` to set up the tools.

.. code-block:: bash

    ### in geomesa-accumulo_2.11-$VERSION/:
    $ bin/geomesa configure
    Warning: GEOMESA_HOME is not set, using /path/to/geomesa-accumulo_2.11-$VERSION
    Using GEOMESA_HOME as set: /path/to/geomesa-accumulo_2.11-$VERSION
    Is this intentional? Y\n y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
    Current value is /path/to/geomesa-accumulo_2.11-$VERSION/lib.

    Is this intentional? Y\n y

    To persist the configuration please update your bashrc file to include:
    export GEOMESA_HOME=/path/to/geomesa-accumulo_2.11-$VERSION
    export PATH=${GEOMESA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_HOME`` and ``$PATH`` updates.

.. warning::

    Please note that the ``$GEOMESA_HOME`` variable points to the location of the ``geomesa-accumulo_2.11-$VERSION``
    directory, not the main geomesa binary distribution directory!

.. note::

    ``geomesa`` will read the ``$ACCUMULO_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Hadoop, Accumulo, Zookeeper, and Thrift. If possible, we recommend
    installing the tools on the Accumulo master server, as you may also need various configuration
    files from Hadoop/Accumulo in order to run certain commands. In addition ``geomesa`` will pull any
    additional jars from the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable into the class path.
    Use the ``geomesa classpath`` command in order to see what JARs are being used.

    If you are running the tools on a system without
    Accumulo installed and configured, the ``install-hadoop-accumulo.sh`` script
    in the ``bin`` directory may be used to download the needed Hadoop/Accumulo JARs into
    the ``lib/common`` directory. You should edit this script to match the versions used by your
    installation.

Due to licensing restrictions, dependencies for shape file support and raster
ingest must be separately installed. Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Test the command that invokes the GeoMesa Tools:


.. _install_hbase_geoserver:

Installing GeoMesa HBase in GeoServer
-------------------------------------

The HBase GeoServer plugin is not bundled by default in a GeoMesa binary distribution
and should be built from source. Download the source distribution (see
:ref:`building_from_source`), go to the ``geomesa-hbase/geomesa-hbase-gs-plugin``
directory, and build the module using the ``hbase`` Maven profile:

.. code-block:: bash

    $ mvn clean install -Phbase

After building, extract ``target/geomesa-hbase-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with HBase 1.1.5
bundled. If you require a different version, modify the ``pom.xml`` and rebuild following
the instructions above.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

 * hadoop-annotations-2.5.1.jar
 * hadoop-auth-2.5.1.jar
 * hadoop-common-2.5.1.jar
 * hadoop-mapreduce-client-core-2.5.1.jar
 * hadoop-yarn-api-2.5.1.jar
 * hadoop-yarn-common-2.5.1.jar
 * zookeeper-3.4.6.jar
 * commons-configuration-1.6.jar

(Note the versions may vary depending on your installation.)

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist).

Restart GeoServer after the JARs are installed.
