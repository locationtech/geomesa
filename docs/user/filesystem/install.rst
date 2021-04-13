Installing GeoMesa FileSystem
=============================

Installing from the Binary Distribution
---------------------------------------

GeoMesa FileSystem artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-fs_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-fs_${VERSION}-bin.tar.gz
    $ cd geomesa-fs_${VERSION}
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

The FileSystem command line tools require Hadoop to run. If ``HADOOP_HOME`` is defined, or ``hadoop`` is available
on the path, the tools will use the local Hadoop installation. Otherwise, when first run they will prompt to download
the necessary JARs. Environment variables can be specified in ``conf/*-env.sh`` and dependency versions can be
specified in ``conf/dependencies.sh``.

Note that you will need the proper Yarn/Hadoop environment configured if you would like to run a distributed ingest
job.

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
``$GEOMESA_FS_HOME/dist/gs-plugins/geomesa-fs-gs-plugin_${VERSION}-install.tar.gz`` into GeoServer's
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
  * hadoop-aws-2.8.4.jar
  * aws-java-sdk-core-1.10.6.jar
  * aws-java-sdk-s3-1.10.6.jar
  * joda-time-2.8.1.jar
  * httpclient-4.5.2.jar
  * httpcore-4.4.4.jar
  * commons-httpclient-3.1.jar

You can use the bundled ``$GEOMESA_FS_HOME/bin/install-dependencies.sh`` script to install these JARs.

The FileSystem data store requires the configuration file ``core-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. code-block:: bash

    $ ln -s /path/to/core-site.xml /path/to/geoserver/WEB-INF/classes/core-site.xml

Restart GeoServer after the JARs are installed.

GeoMesa Process
^^^^^^^^^^^^^^^

GeoMesa provides some WPS processes, such as ``geomesa:Density`` which is used to generate heat maps. In order
to use these processes, install the GeoServer WPS plugin as described in :ref:`geomesa_process`.
