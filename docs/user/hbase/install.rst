Installing GeoMesa HBase
========================

.. note::

    GeoMesa currently supports HBase version |hbase_version|.

GeoMesa supports traditional HBase installations as well as HBase running on `Amazon's EMR <https://aws.amazon.com/emr/>`_
, `Hortonworks' Data Platform (HDP) <https://hortonworks.com/products/data-center/hdp/>`_, and the
`Cloudera Distribution of Hadoop (CDH) <https://www.cloudera.com/products/enterprise-data-hub.html>`_. For details
on bootstrapping an EMR cluster, see :doc:`/tutorials/geomesa-hbase-s3-on-aws`. For details on deploying to
Cloudera CDH, see :doc:`/tutorials/geomesa-hbase-on-cdh`.

Installing the Binary Distribution
----------------------------------

GeoMesa HBase artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-hbase_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-hbase_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-hbase_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _hbase_install_source:

Building from Source
--------------------

GeoMesa HBase may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa HBase
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-hbase/geomesa-hbase-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _hbase_deploy_distributed_runtime:

Installing the GeoMesa Distributed Runtime JAR
----------------------------------------------

GeoMesa uses custom HBase filters and coprocessors to speed up queries. In order to use them, you must deploy the
distributed runtime jar to the HBase to the directory specified by the HBase configuration variable called
``hbase.dynamic.jars.dir``.  This is set to ``${hbase.rootdir}/lib`` by default.  Copy the distribute runtime jar to
this directory as follows:

.. code-block:: bash

    hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-$VERSION.jar ${hbase.dynamic.jars.dir}/

If running on top of Amazon S3, you will need to use the ``aws s3`` command line tool.

.. code-block:: bash

    aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-$VERSION.jar s3://${hbase.dynamic.jars.dir}/

If required, you may disable distributed processing by setting the system property ``geomesa.hbase.remote.filtering``
to ``false``. Note that this may have an adverse effect on performance.

.. _registering_coprocessors:

Register the Coprocessors
^^^^^^^^^^^^^^^^^^^^^^^^^

Assuming that you have installed the distributed runtime JAR under ``hbase.dynamic.jars.dir``, coprocessors will be
registered automatically when you call ``createSchema`` on a data store. Alternatively, the coprocessors may be
registered manually. See :ref:`coprocessor_alternate` for details.

For more information on managing coprocessors see
`Coprocessor Introduction <https://blogs.apache.org/hbase/entry/coprocessor_introduction>`_ on Apache's Blog.

Configuration and Classpaths
----------------------------

GeoMesa HBase requires Hadoop and HBase jars and configuration files to be available on the classpath. This includes
files such as the hbase-site.xml and core-site.xml files in addition to standard jars and libraries. Configuring the
classpath is important if you plan to use the GeoMesa HBase command line tools to ingest and manage GeoMesa.

By default, GeoMesa HBase will attempt to read various HBase and Hadoop related environmental variables in order to
build the classpath. You can configure environment variables and classpath settings in
``geomesa-hbase_2.11-$VERSION/conf/geomesa-env.sh`` or in your external env (e.g. bashrc file). The logic GeoMesa
uses to determine which external entries to include on the classpath is:

    1. If the environmental variables ``GEOMESA_HADOOP_CLASSPATH`` and ``GEOMESA_HBASE_CLASSPATH`` are set then GeoMesa
    HBase will use these variables to set the classpath and skip all other logic.

    2. Next, if ``$HBASE_HOME`` and ``$HADOOP_HOME`` are set then GeoMesa HBase will attempt to build the classpath by
    searching for jar files and configuration in standard locations. Note that this is very specific to the
    installation or distribution of Hadoop you are using and may not be reliable.

    3. If no environmental variables are set but the ``hbase`` and ``hadoop`` commands are available then GeoMesa will
    interrogate them for their classpaths by running the ``hadoop classpath`` and ``hbase classpath`` commands. This
    method of classpath determination is slow due to the fact that the ``hbase classpath`` command forks a new JVM. It
    is therefore recommended that you set manually set these variables in your environment or the
    ``conf/geomesa-env.sh`` file.

In addition, ``geomesa-hbase`` will pull any additional entries from the ``GEOMESA_EXTRA_CLASSPATHS``
environment variable.

Note that the ``GEOMESA_EXTRA_CLASSPATHS``, ``GEOMESA_HADOOP_CLASSPATH``, and ``GEOMESA_HBASE_CLASSPATH`` variables
all follow standard
`Java Classpath <http://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html>`_ conventions, which
generally means that entries must be directories, JAR, or zip files. Individual XML files will be ignored. For example,
to add a ``hbase-site.xml`` or ``core-site.xml`` file to the classpath you must either include a directory on the
classpath or add the file to a zip or JAR archive to be included on the classpath.

Use the ``geomesa classpath`` command in order to see what JARs are being used.

A few suggested configurations are below:

.. tabs::

    .. group-tab:: Amazon EMR

        When using EMR to install HBase or Hadoop there are AWS specific jars that need to be used (e.g. EMR FS).
        It is recommended to use EMR to install Hadoop and/or HBase in order to properly configure and install these
        dependencies (especially when using HBase on S3).

        If you used EMR to install Hadoop and HBase, you can view their classpaths using the ``hadoop classpath`` and
        ``hbase classpath`` commands to build an appropriate classpath to include jars and configuration files for
        GeoMesa HBase:

        .. code-block:: bash

            export GEOMESA_HADOOP_CLASSPATH=$(hadoop classpath)
            export GEOMESA_HBASE_CLASSPATH=$(hbase classpath)
            export GEOMESA_HBASE_HOME=/opt/geomesa
            export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"

    .. group-tab:: Standard

        Configure GeoMesa to use pre-installed HBase and Hadoop distributions:

        .. code-block:: bash

            export HADOOP_HOME=/path/to/hadoop
            export HBASE_HOME=/path/to/hbase
            export GEOMESA_HBASE_HOME=/opt/geomesa
            export PATH="${PATH}:${GEOMESA_HOME}/bin"

    .. group-tab:: HDP

        Configure the environment to use an HDP install

        .. code-block:: bash

            export HADOOP_HOME=/usr/hdp/current/hadoop-client/
            export HBASE_HOME=/usr/hdp/current/hbase-client/
            export GEOMESA_HBASE_HOME=/opt/geomesa
            export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"

    .. group-tab:: Manual Install

        If no HBase or Hadoop distribution is installed, try manually installing the JARs from maven:

        .. code-block:: bash

            export GEOMESA_HBASE_HOME=/opt/geomesa
            export PATH="${PATH}:${GEOMESA_HBASE_HOME}/bin"
            cd GEOMESA_HBASE_HOME
            bin/install-hadoop.sh lib
            bin/install-hbase.sh lib

        You will also need to provide the hbase-site.xml file within a the GeoMesa ``conf`` directory, an external
        directory, zip, or JAR archive (an entry referencing the XML file directly will not work with the Java
        classpath). 

        When creating a zip or jar file, the hbase-site.xml should be at the root level of the archive
        and not nested within any packages or subfolders. For example:

        .. code-block:: bash

            $ jar tf my.jar
            META-INF/
            META-INF/MANIFEST.MF
            hbase-site.xml 

        .. code-block:: bash

            # try this
            cp /path/to/hbase-site.xml ${GEOMESA_HBASE_HOME}/conf/

            # or this
            cd /path/to/hbase-conf-dir
            jar cvf conf.jar hbase-site.xml
            export GEOMESA_EXTRA_CLASSPATHS=/path/to/confdir:/path/to/conf.zip:/path/to/conf.jar


Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

.. _setting_up_hbase_commandline:

Setting up the HBase Command Line Tools
---------------------------------------

.. warning::

    To use HBase with the command line tools, you need to install the coprocessors first, as described above.

GeoMesa comes with a set of command line tools for managing HBase features located in
``geomesa-hbase_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in geomesa-hbase_2.11-$VERSION/conf/geomesa-env.sh.

If desired, you may use the included script ``bin/geomesa-hbase configure`` to help set up the environment variables
used by the tools. Otherwise, you may invoke the ``geomesa-hbase`` script using the fully-qualified path, and
use the default configuration.

The tools will read the ``$HBASE_HOME`` and ``$HADOOP_HOME`` environment variables to load the
appropriate JAR files for HBase and Hadoop. If installing on a system without HBase and/or Hadoop,
the ``install-hbase.sh`` and ``install-hadoop.sh`` scripts in the ``bin`` directory may be used to download
the required HBase and Hadoop JARs into the ``lib`` directory. You should edit this script to match the versions
used by your installation.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

.. note::

    GeoMesa provides the ability to provide additional jars on the classpath using the environmental variable
    ``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
    classpath giving it highest precedence in the classpath. Users can provide directories of jar files or individual
    files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.
    Use the ``geomesa-hbase classpath`` command to print the final classpath that will be used when executing geomesa
    commands.

The tools also need access to the ``hbase-site.xml`` for your cluster. If ``$HBASE_HOME`` is defined, it will pick
it up from there. However, it may not be available for map/reduce jobs. To ensure it's availability,
add it at the root level of the ``geomesa-hbase-datastore`` JAR in the lib folder:

.. code-block:: bash

    $ zip -r lib/geomesa-hbase-datastore_2.11-$VERSION.jar hbase-site.xml

.. warning::

    Ensure that the ``hbase-site.xml`` is at the root (top) level of your JAR, otherwise it will not be picked up.

Due to licensing restrictions, certain dependencies for shape file support must be separately
installed. Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Test the command that invokes the GeoMesa Tools:

.. code::

    $ bin/geomesa-hbase
    INFO  Usage: geomesa-hbase [command] [command options]
      Commands:
      ...

For more details, see :ref:`hbase_tools`.

.. _install_hbase_geoserver:

Installing GeoMesa HBase in GeoServer
-------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

The HBase GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_HBASE_HOME/dist/gs-plugins/geomesa-hbase-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with HBase |hbase_bundled_version|
bundled. This JAR should work with HBase |hbase_version|.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

(Note the versions may vary depending on your installation.)

.. tabs::

    .. group-tab:: Standard

        * commons-cli-1.2.jar
        * commons-configuration-1.6.jar
        * commons-io-2.5.jar  (you may need to remove an older version from geoserver)
        * commons-logging-1.1.3.jar
        * hadoop-auth-2.7.4.jar
        * hadoop-client-2.7.4.jar
        * hadoop-common-2.7.4.jar
        * hadoop-hdfs-2.7.4.jar
        * htrace-core-3.1.0-incubating.jar
        * metrics-core-2.2.0.jar
        * netty-3.6.2.Final.jar
        * netty-all-4.0.41.Final.jar
        * servlet-api-2.4.jar
        * zookeeper-3.4.10.jar

        You can use the bundled ``$GEOMESA_HBASE_HOME/bin/install-hadoop.sh`` script to install these JARs.

    .. group-tab:: HDP

        * hadoop-annotations.jar
        * hadoop-auth.jar
        * hadoop-common.jar
        * protobuf-java.jar
        * commons-io.jar
        * zookeeper-3.4.10.jar
        * commons-configuration-1.6.jar

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. tabs::

    .. group-tab:: Standard

        .. code-block:: bash

            ln -s /path/to/hbase-site.xml /path/to/geoserver/WEB-INF/classes/hbase-site.xml

    .. group-tab:: HDP

        .. code-block:: bash

            ln -s /usr/hdp/current/hbase-client/hbase-site.xml /path/to/geoserver/WEB-INF/classes/hbase-site.xml

Restart GeoServer after the JARs are installed.

Connecting to External HBase Clusters Backed By S3
--------------------------------------------------

To use a EMR cluster to connect to an existing, external HBase Cluster first follow the above instructions to setup the
new cluster and install GeoMesa.

The next step is to obtain the ``hbase-site.xml`` for the external HBase Cluster, copy to the new EMR cluster and
copy it into ``${GEOMESA_HBASE_HOME}/conf``. At this point you may run the geomesa-hbase command line tools.

If you wish to execute SQL queries using Spark, you must first zip the ``hbase-site.xml`` file for the external cluster:

.. code-block:: shell

    zip hbase-site.zip hbase-site.xml

Then copy the zip file to  ``${GEOMESA_HBASE_HOME}/conf`` then add the zipped configuration file to the Spark classpath:

.. code-block:: shell

    export SPARK_JARS=file:///opt/geomesa/dist/spark/geomesa-hbase-spark-runtime_2.11-${VERSION}.jar,file:///opt/geomesa/conf/hbase-site.zip

Then start up the Spark shell:

.. code-block:: shell

    spark-shell --jars $SPARK_JARS

Configuring HBase on Azure HDInsight
------------------------------------

HDInsight generally creates ``HBASE_HOME`` in HDFS under the path ``/hbase``. In order to make the GeoMesa
coprocessors and filters available to the region servers, use the ``hadoop`` filesystem command to put
the GeoMesa JAR there:

.. code-block:: shell

    hadoop fs -mkdir /hbase/lib
    hadoop fs -put geomesa-hbase-distributed-runtime-$VERSION.jar /hbase/lib/
