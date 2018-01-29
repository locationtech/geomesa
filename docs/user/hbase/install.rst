Installing GeoMesa HBase
========================

GeoMesa supports traditional HBase installations as well as HBase running on `Amazon's EMR <https://aws.amazon.com/emr/>`_
, `Hortonworks' Data Platform (HDP) <https://hortonworks.com/products/data-center/hdp/>`_, and the
`Cloudera Distribution of Hadoop (CDH) <https://www.cloudera.com/products/enterprise-data-hub.html>`_. For details
on bootstrapping an EMR cluster, see :doc:`/tutorials/geomesa-hbase-s3-on-aws`. For details on deploying to
Cloudera CDH, see :doc:`/tutorials/geomesa-hbase-on-cdh`.

.. _setting_up_hbase_commandline:

Installing the Binary Distribution
----------------------------------

GeoMesa HBase artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-hbase-dist_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-hbase-dist_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

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

.. _hbase_deploy_distributed_runtime:

Deploying the GeoMesa HBase distributed runtime jar
---------------------------------------------------

GeoMesa uses an HBase custom filter to improve processing of CQL queries.  In order to use the custom filter, you must
deploy the distributed runtime jar to the HBase to the directory specified by the HBase configuration variable called
``hbase.dynamic.jars.dir``.  This is set to ``${hbase.rootdir}/lib`` by default.  Copy the distribute runtime jar to
this directory as follows:

.. code-block:: bash

    hadoop fs -put ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-$VERSION.jar ${hbase.dynamic.jars.dir}/

If running on top of Amazon S3, you will need to use the ``aws s3`` command line tool.

.. code-block:: bash

    aws s3 cp ${GEOMESA_HBASE_HOME}/dist/hbase/geomesa-hbase-distributed-runtime-$VERSION.jar s3://${hbase.dynamic.jars.dir}/

If required, you may disable distributed processing by setting the system property ``geomesa.hbase.remote.filtering``
to ``false``. Note that this may have an adverse effect on performance.

.. _hbase_install_source:

Building from Source
--------------------

GeoMesa HBase may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa HBase
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-hbase/geomesa-hbase-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _registering_coprocessors:

Register the Coprocessors
-------------------------

GeoMesa utilizes server side processing to accelerate some queries. Currently the only processing done server side is
density (heatmap) calculations. In order to utilize this feature the GeoMesa coprocessor must be registered on all GeoMesa tables
or registered site-wide and the ``geomesa-hbase-distributed-runtime`` code must be available on the classpath or at an
HDFS url, depending on the registration method used.

There are a number of ways to register the coprocessors, which are detailed later.

The following ways to register coprocessors can be done anytime and constitute the 'upgrade path', however, they may
require HBase or tables to be taken offline.

 * Register Site-Wide using the ``hbase-site.xml``
 * Register Per-Table using the ``hbase shell``

The following ways to register coprocessors must be done **before** the tables are created.

 * Classpath Auto-Registration
 * System Property or geomesa-site.xml
 * DataStore Param Registration

There are two ways to get the coprocessor code on the classpath.

 * Modify the ``hbase-env.sh`` file and provide the path to the ``geomesa-hbase-distributed-runtime`` JAR in the
   ``HBASE_CLASSPATH`` property. If this method is used, the ``geomesa-hbase-distributed-runtime`` JAR must be available at
   the given location on all master and region servers.
 * If registering the coprocessors on a per-table basis using the hbase shell, it is possible to provide the HDFS path to the
   ``geomesa-hbase-distributed-runtime`` JAR that was deployed in :ref:`hbase_deploy_distributed_runtime`.

.. tabs::

    .. tab:: Site-Wide

        The easiest method to register the coprocessors is to specify the coprocessors in the ``hbase-site.xml``.
        To do this simply add the coprocessors classname to the ``hbase.coprocessor.user.region.classes`` key.

        .. code-block:: xml

            <configuration>
              <property>
                <name>hbase.coprocessor.user.region.classes</name>
                <value>org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor</value>
              </property>
            </configuration>

        All new and existing non-system tables will have access to the GeoMesa Coprocessor.

    .. tab:: Per-Table

        If your hbase instance is used for more than GeoMesa table or would like to utilize HDFS to deploy the
        ``geomesa-hbase-distributed-runtime`` JAR or for some other reason do not wish to register the coprocessor
        site wide you may configure the coprocessor on a per-table basis. This can be done by utilizing the the hbase shell
        as shown below. When specifying a coprocessor, the coprocessor must be available on the HBase classpath on all
        of the master and region servers or you must provide the HDFS URL for the ``geomesa-hbase-distributed-runtime`` JAR that
        was deployed in :ref:`hbase_deploy_distributed_runtime`.

        To run the hbase shell simply execute:

        .. code-block:: bash

            $ ${HBASE_HOME}/bin/hbase shell
            HBase Shell; enter 'help<RETURN>' for list of supported commands.
            Type "exit<RETURN>" to leave the HBase Shell
            hbase(main):001:0>

        To get a list of the current tables run:

        .. code-block:: bash

            hbase(main):001:0> list
            TABLE
            geomesa
            geomesa_QuickStart_id
            geomesa_QuickStart_z2
            geomesa_QuickStart_z3
            4 row(s) in 0.1380 seconds

        You will need to install the coprocessor on all table indexes list. The ``geomesa`` table in this example is the metadata
        table and does not need the coprocessor installed.

        We use the ``alter`` command to modify the configuration of the tables. The ``coprocessor`` parameter in the ``alter``
        command may be modified to change the registration of the GeoMesa coprocessors.

        .. code-block:: bash

            'coprocessor'=>'HDFS_URL|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor|PRIORITY|'

        The 'value' of the ``coprocessor`` parameter has four parts, separated by ``|``, two of which, ``HDFS_URL`` and
        ``PRIORITY``, are configurable depending on your environment.

         * To provide the HDFS URL of the ``geomesa-hbase-distributed-runtime`` JAR replace HDFS_URL in the coprocessor value with the
           HDFS URL. This is only need if the ``geomesa-hbase-distributed-runtime`` JAR will not be on the classpath by other means.
         * To alter the priority (execution order) of the coprocessor change PRIRORITY to the desired value, this is optional and
           should be left blank if now used.

        .. code-block:: bash

            hbase(main):040:0> alter 'geomesa_QuickStart_id', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor||'
            Updating all regions with the new schema...
            22/22 regions updated.
            Done.
            0 row(s) in 5.0000 seconds

            hbase(main):041:0> alter 'geomesa_QuickStart_z2', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor||'
            Updating all regions with the new schema...
            4/4 regions updated.
            Done.
            0 row(s) in 2.8850 seconds

            hbase(main):042:0> alter 'geomesa_QuickStart_z3', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor||'
            Updating all regions with the new schema...
            4/4 regions updated.
            Done.
            0 row(s) in 2.9150 seconds

        To verify this worked successfully, run:

        .. code-block:: bash

            hbase(main):002:0> describe 'TABLE_NAME'
            Table TABLE_NAME is ENABLED
            TABLE_NAME, {TABLE_ATTRIBUTES => {coprocessor$1 => '|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor||'}
            COLUMN FAMILIES DESCRIPTION
            {NAME => 'm', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_EN
            CODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '655
            36', REPLICATION_SCOPE => '0'}
            1 row(s) in 0.1940 seconds

    .. tab:: Classpath

        If the ``geomesa-hbase-distributed-runtime`` JAR is available on the HBase classpath when the table is created then the
        GeoMesa coprocessors will be automatically registered for that table.

    .. tab:: System-Property

        System Property or geomesa-site.xml are essentially the same as they utilize the same mechanism, but two
        different approaches.

        If the Java system property ``geomesa.hbase.coprocessor.path`` is set in the environment running the GeoMesa ingest
        then the HDFS or S3 URL provided as the value will be automatically registered in the table descriptor. There are three
        to do this.

        * Set the system property in your shell environment using the ``JAVA_TOOL_OPTIONS`` environment variable.

        .. code-block:: bash

            export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dgeomesa.hbase.coprocessor.path=hdfs://path/to/geomesa-runtime.jar"

        * Set the system property in the ``geomesa-env.sh`` script.

        .. code-block:: bash

            setvar CUSTOM_JAVA_OPTS "${JAVA_OPTS} -Dgeomesa.hbase.coprocessor.path=hdfs://path/to/geomesa-runtime.jar"

        * Set the system property using the ``geomesa-site.xml`` configuration file.

        .. code-block:: xml

            <property>
                <name>geomesa.hbase.coprocessor.path</name>
                <value>hdfs://path/to/geomesa-runtime.jar</value>
                <description>HDFS or local path to GeoMesa-HBase Coprocessor JAR. If a local path is provided it must be the same for
                    all region server. A path provided through the DataStore parameters will always override this property.
                </description>
                <final>false</final>
            </property>

    .. tab:: DS-Parameter

        If you are using GeoMesa-HBase programmatically you can use the datastore parameter ``coprocessor.url`` to set an HDFS or
        S3 path to the ``geomesa-hbase-distributed-runtime`` JAR.

For more information on managing coprocessors see
`Coprocessor Introduction <https://blogs.apache.org/hbase/entry/coprocessor_introduction>`_ on Apache's Blog.

.. _install_hbase_geoserver:

Installing GeoMesa HBase in GeoServer
-------------------------------------

The HBase GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_HBASE_HOME/dist/gs-plugins/geomesa-hbase-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with HBase 1.2.3
bundled. If you require a different version, modify the ``pom.xml`` and build the GeoMesa HBase plugin project from
scratch with Maven.

This distribution does not include the Hadoop or Zookeeper JARs; the following JARs
should be copied from the ``lib`` directory of your HBase or Hadoop installations into
GeoServer's ``WEB-INF/lib`` directory:

(Note the versions may vary depending on your installation.)

.. tabs::

    .. group-tab:: Standard

        * hadoop-annotations-2.7.4.jar
        * hadoop-auth-2.7.4.jar
        * hadoop-common-2.7.4.jar
        * hadoop-mapreduce-client-core-2.7.4.jar
        * hadoop-yarn-api-2.7.4.jar
        * hadoop-yarn-common-2.7.4.jar
        * htrace-core-3.1.0-incubating.jar
        * commons-cli-1.2.jar
        * commons-io-2.5.jar (you may need to remove an older version (2.1) from geoserver)
        * hbase-common-1.2.6.jar
        * hbase-client-1.2.6.jar
        * hbase-server-1.2.6.jar
        * hbase-protocol-1.2.6.jar
        * metrics-core-2.2.0.jar
        * netty-3.6.2.Final.jar
        * netty-all-4.0.41.Final.jar
        * zookeeper-3.4.10.jar
        * commons-configuration-1.6.jar

        You can use the bundled ``$GEOMESA_HBASE_HOME/bin/install-hadoop.sh`` script to install these JARs.

    .. group-tab:: HDP

        * hadoop-annotations.jar
        * hadoop-auth.jar
        * hadoop-common.jar
        * protobuf-java.jar
        * commons-io.jar
        * hbase-server-1.2.6.jar
        * zookeeper-3.4.10.jar
        * commons-configuration-1.6.jar

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be use full here so any changes are reflected in GeoServer.

.. tabs::

    .. group-tab:: Standard

        .. code-block:: bash

            ln -s /path/to/hbase-site.xml /path/to/geoserver/WEB-INF/classes/hbase-site.xml

    .. group-tab:: HDP

        .. code-block:: bash

            ln -s /usr/hdp/current/hbase-client/hbase-site.xml /path/to/geoserver/WEB-INF/classes/hbase-site.xml

Restart GeoServer after the JARs are installed.

Jackson Version
^^^^^^^^^^^^^^^

.. warning::

    Some GeoMesa functions (in particular Arrow conversion) requires ``jackson-core-2.6.x``. Some versions
    of GeoServer ship with an older version, ``jackson-core-2.5.0.jar``. After installing the GeoMesa
    GeoServer plugin, be sure to delete the older JAR from GeoServer's ``WEB-INF/lib`` folder.

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
