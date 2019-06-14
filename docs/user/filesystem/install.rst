Installing GeoMesa FileSystem
=============================

Installing from the Binary Distribution
---------------------------------------

GeoMesa FileSystem artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-fs_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-fs_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-fs_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _fsds_install_source:

Building from Source
--------------------

GeoMesa FileSystem may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-fs/geomesa-fs-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_fsds_commandline:

Setting up the FileSystem Command Line Tools
--------------------------------------------

After untaring the distribution, you'll need to either define the standard Hadoop environment variables or install Hadoop
using the ``bin/install-hadoop.sh`` script provided in the tarball. Note that you will need the proper Yarn/Hadoop
environment configured if you would like to run a distributed ingest job to create files.

If you are using a service such as Amazon Elastic MapReduce (EMR) or have a distribution of Apache Hadoop, Cloudera, or
Hortonworks installed you can likely run something like this to configure hadoop for the tools:

.. code-block:: bash

    # These will be specific to your Hadoop environment
    . /etc/hadoop/conf/hadoop-env.sh
    . /etc/hadoop/conf/yarn-env.sh
    export HADOOP_CONF_DIR=/etc/hadoop/conf

After installing the tarball you should be able to run the ``geomesa-fs`` command like this:

.. code-block:: bash

    $ cd $GEOMESA_FS_HOME
    $ bin/geomesa-fs

The output should look like this::

    INFO  Usage: geomesa-fs [command] [command options]
      Commands:
        ...

.. _install_fsds_geoserver:

Installing GeoMesa FileSystem in GeoServer
------------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

The FileSystem GeoServer plugin is bundled by default in the GeoMesa FS binary distribution. To install, extract
``$GEOMESA_FS_HOME/dist/gs-plugins/geomesa-fs-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with Parquet 1.9.0
bundled. If you require a different version, modify the ``pom.xml`` and build the GeoMesa FileSystem geoserver plugin
project from scratch with Maven.

This distribution does not include the Hadoop JARs; the following JARs should be copied from the ``lib`` directory of
your Hadoop installations into GeoServer's ``WEB-INF/lib`` directory:

(Note the versions may vary depending on your installation.)

  * hadoop-annotations-2.7.3.jar
  * hadoop-auth-2.7.3.jar
  * hadoop-common-2.7.3.jar
  * hadoop-mapreduce-client-core-2.7.3.jar
  * hadoop-yarn-api-2.7.3.jar
  * hadoop-yarn-common-2.7.3.jar
  * commons-configuration-1.6.jar

You can use the bundled ``$GEOMESA_FS_HOME/bin/install-hadoop.sh`` script to install these JARs.

The FileSystem data store requires the configuration file ``core-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. code-block:: bash

    $ ln -s /path/to/core-site.xml /path/to/geoserver/WEB-INF/classes/core-site.xml

Restart GeoServer after the JARs are installed.
