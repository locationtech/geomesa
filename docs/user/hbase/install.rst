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
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _hbase_deploy_distributed_runtime:

Deploying the GeoMesa HBase distributed runtime jar
---------------------------------------------------

GeoMesa uses an HBase custom filter to improve processing of CQL queries.  In order to use the custom filter, you must
deploy the distributed runtime jar to the HBase to the directory specified by the HBase configuration variable called
``hbase.dynamic.jars.dir``.  This is set to ``${hbase.rootdir}/lib`` by default.  Copy the distribute runtime jar to
this directory as follows:

.. code-block:: bash

    $ hadoop fs -cp ${GEOMESA_HOME}/dist/geomesa-hbase-distributed-runtime-$VERSION.jar ${hbase.dynamic.jars.dir}/

If running on top of Amazon S3, you will need to use the ``aws s3`` command line tool.

.. code-block:: bash

    $ aws s3 cp ${GEOMESA_HOME}/dist/geomesa-hbase-distributed-runtime-$VERSION.jar s3://${hbase.dynamic.jars.dir}/

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

 * :ref:`register_site-wide` using the ``hbase-site.xml``
 * :ref:`register_per-table` using the ``hbase shell``

The following ways to register coprocessors must be done **before** the tables are created.

 * :ref:`classpath-auto-registration`
 * :ref:`system-property-registration`
 * :ref:`datastore-param-registration`

There are two ways to get the coprocessor code on the classpath.

 * Modify the ``hbase-env.sh`` file and provide the path to the ``geomesa-hbase-distributed-runtime`` JAR in the
   ``HBASE_CLASSPATH`` property. If this method is used, the ``geomesa-hbase-distributed-runtime`` JAR must be available at
   the given location on all master and region servers.
 * If registering the coprocessors on a per-table basis using the hbase shell, it is possible to provide the HDFS path to the
   ``geomesa-hbase-distributed-runtime`` JAR that was deployed in :ref:`hbase_deploy_distributed_runtime`.

.. _register_site-wide:

Register Site-Wide
^^^^^^^^^^^^^^^^^^

The easiest method to register the coprocessors is to specify the coprocessors in the ``hbase-site.xml``.
To do this simply add the coprocessors classname to the ``hbase.coprocessor.user.region.classes`` key.

.. code-block:: xml

    <configuration>
      <property>
        <name>hbase.coprocessor.user.region.classes</name>
        <value>org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor</value>
      </property>
    </configuration>

All new and existing non-system tables will have access to the GeoMesa Coprocessor.

.. _register_per-table:

Register Per-Table
^^^^^^^^^^^^^^^^^^

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

    'coprocessor'=>'HDFS_URL|org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor|PRIORITY|'

The 'value' of the ``coprocessor`` parameter has four parts, separated by ``|``, two of which, ``HDFS_URL`` and
``PRIORITY``, are configurable depending on your environment.

 * To provide the HDFS URL of the ``geomesa-hbase-distributed-runtime`` JAR replace HDFS_URL in the coprocessor value with the
   HDFS URL. This is only need if the ``geomesa-hbase-distributed-runtime`` JAR will not be on the classpath by other means.
 * To alter the priority (execution order) of the coprocessor change PRIRORITY to the desired value, this is optional and
   should be left blank if now used.

.. code-block:: bash

    hbase(main):040:0> alter 'geomesa_QuickStart_id', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor||'
    Updating all regions with the new schema...
    22/22 regions updated.
    Done.
    0 row(s) in 5.0000 seconds

    hbase(main):041:0> alter 'geomesa_QuickStart_z2', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor||'
    Updating all regions with the new schema...
    4/4 regions updated.
    Done.
    0 row(s) in 2.8850 seconds

    hbase(main):042:0> alter 'geomesa_QuickStart_z3', METHOD => 'table_att', 'coprocessor'=>'|org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor||'
    Updating all regions with the new schema...
    4/4 regions updated.
    Done.
    0 row(s) in 2.9150 seconds

To verify this worked successfully, run:

.. code-block:: bash

    hbase(main):002:0> describe 'TABLE_NAME'
    Table TABLE_NAME is ENABLED
    TABLE_NAME, {TABLE_ATTRIBUTES => {coprocessor$1 => '|org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor
    ||'}
    COLUMN FAMILIES DESCRIPTION
    {NAME => 'm', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_EN
    CODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '655
    36', REPLICATION_SCOPE => '0'}
    1 row(s) in 0.1940 seconds

.. _classpath-auto-registration:

Classpath Auto-Registration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the ``geomesa-hbase-distributed-runtime`` JAR is available on the HBase classpath when the table is created then the
GeoMesa coprocessors will be automatically registered for that table.

.. _system-property-registration:

System Property or geomesa-site.xml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These two methods are essentially the same as they utilize the same mechanism, but two different approaches.

If the Java system property ``geomesa.hbase.coprocessor.path`` is set in the environment running the GeoMesa ingest
then the HDFS or S3 URL provided as the value will be automatically registered in the table descriptor. There are three
to do this.

 * Set the system property in your shell environment using the ``JAVA_OPTS`` environment variable.

.. code-block:: bash

    export JAVA_OPTS="${JAVA_OPTS} -Dgeomesa.hbase.coprocessor.path=hdfs://path/to/geomesa-runtime.jar"

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

.. _datastore-param-registration:

DataStore Param Registration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are using GeoMesa-HBase programmatically you can use the datastore parameter ``coprocessors.url`` to set an HDFS or
S3 path to the ``geomesa-hbase-distributed-runtime`` JAR.

For more information on managing coprocessors see
`Coprocessor Introduction <https://blogs.apache.org/hbase/entry/coprocessor_introduction>`_ on Apache's Blog.

.. _setting_up_hbase_commandline:

Setting up the HBase Command Line Tools
---------------------------------------

GeoMesa HBase comes with a set of command line tools for managing HBase features located in
``geomesa-hbase_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in ``geomesa-hbase_2.11-$VERSION/conf/geomesa-env.sh``.

In the ``geomesa-hbase_2.11-$VERSION`` directory, run ``bin/geomesa-hbase configure`` to set up the tools.

.. code-block:: bash

    $ bin/geomesa-hbase configure
    Using GEOMESA_HBASE_HOME = /path/to/geomesa-hbase_2.11-1.3.0
    Do you want to reset this? Y\n y
    Using GEOMESA_HBASE_HOME as set: /path/to/geomesa-hbase_2.11-1.3.0
    Is this intentional? Y\n y
    To persist the configuration please edit conf/geomesa-env.sh or update your bashrc file to include:
    export GEOMESA_HBASE_HOME=/path/to/geomesa-hbase_2.11-1.3.0
    export PATH=${GEOMESA_HBASE_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_HBASE_HOME`` and ``$PATH`` updates.

.. note::

    ``geomesa-hbase`` will read the ``$HBASE_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Hadoop and HBase. In addition, ``geomesa-hbase`` will pull any
    additional entries from the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable.
    Use the ``geomesa classpath`` command in order to see what JARs are being used.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Run ``geomesa-hbase`` without arguments to confirm that the tools work.

.. code::

    $ bin/geomesa-hbase
    Using GEOMESA_HBASE_HOME = /path/to/geomesa-hbase_2.11-1.3.0
    INFO  Usage: geomesa-hbase [command] [command options]
      Commands:
      ...

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

 * hadoop-annotations-2.7.3.jar
 * hadoop-auth-2.7.3.jar
 * hadoop-common-2.7.3.jar
 * hadoop-mapreduce-client-core-2.7.3.jar
 * hadoop-yarn-api-2.7.3.jar
 * hadoop-yarn-common-2.7.3.jar
 * hbase-server-1.2.6.jar
 * zookeeper-3.4.9.jar
 * commons-configuration-1.6.jar

(Note the versions may vary depending on your installation.)

You can use the bundled ``$GEOMESA_HBASE_HOME/bin/install-hadoop.sh`` script to install these JARs.

The HBase data store requires the configuration file ``hbase-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist).

Restart GeoServer after the JARs are installed.
