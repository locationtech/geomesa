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
using the ``bin/install-hadoop.sh`` script provided in the tarball. If using AWS S3 as the filesystem, run ``bin/install-s3.sh``. Note that you will need the proper Yarn/Hadoop
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

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The FileSystem GeoServer plugin is bundled by default in the GeoMesa FS binary distribution. To install, extract
``$GEOMESA_FS_HOME/dist/gs-plugins/geomesa-fs-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with Parquet 1.9.0
bundled. If you require a different version, modify the ``pom.xml`` and build the GeoMesa FileSystem geoserver plugin
project from scratch with Maven.

This distribution does not include the Hadoop JARs; the following JARs should be copied from the ``lib`` directory of
your Hadoop installations into GeoServer's ``WEB-INF/lib`` directory:

(Note the versions may vary depending on your installation.)

  * hadoop-auth-2.8.4.jar
  * hadoop-common-2.8.4.jar
  * hadoop-hdfs-2.8.4.jar
  * hadoop-hdfs-client-2.8.4.jar
  * snappy-java-1.1.1.6.jar
  * commons-configuration-1.6.jar
  * commons-logging-1.1.3.jar
  * commons-cli-1.2.jar
  * commons-io-2.5.jar
  * protobuf-java-2.5.0.jar


You can use the bundled ``$GEOMESA_FS_HOME/bin/install-hadoop.sh`` script to install these JARs.

For AWS S3 functionality, run the bundled ``$GEOMESA_FS_HOME/bin/install-s3.sh`` script to install the following jars:

(Note the versions may vary depending on your installation.)

  * hadoop-aws-2.8.4.jar
  * aws-java-sdk-core-1.10.6.jar
  * aws-java-sdk-s3-1.10.6.jar
  * joda-time-2.8.1.jar
  * httpclient-4.3.4.jar
  * httpcore-4.3.3.jar
  * commons-httpclient-3.1.jar


These JARs should be copied from ``$GEOMESA_FS_HOME/lib/`` into GeoServer's ``WEB-INF/lib`` directory.

The FileSystem data store requires the configuration file ``core-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. code-block:: bash

    $ ln -s /path/to/core-site.xml /path/to/geoserver/WEB-INF/classes/core-site.xml

Restart GeoServer after the JARs are installed.

GeoMesa Process
^^^^^^^^^^^^^^^

GeoMesa-specific WPS processes (such as ``geomesa:Density``, which is used to generate heat maps), require the
installation of the ``geomesa-process-wps_2.11-$VERSION.jar`` in ``geoserver/WEB-INF/lib``. This JAR is included
in the ``geomesa-fs_2.11-$VERSION/dist/gs-plugins`` directory of the binary distribution, or is built in the
``geomesa-process`` module of the source distribution.

.. note::

  The WPS JAR requires the installation of the
  `GeoServer WPS Plugin <http://docs.geoserver.org/stable/en/user/services/wps/install.html>`__.
