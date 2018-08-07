.. _coprocessor_alternate:

Manual Coprocessors Registration
================================

In most cases, you don't need to do anything to register coprocessors. If you have installed the GeoMesa
distributed runtime JAR under , as detailed in :ref:`registering_coprocessors`, then coprocessors will be
registered automatically.

However, in some situations, you may wish to register the coprocessors in a different way.

Register Site-Wide
------------------

It is possible to register the coprocessors in the main ``hbase-site.xml``. To do this simply add the coprocessor
classnames to the ``hbase.coprocessor.user.region.classes`` key. Note that this requires HBase to be taken offline.

.. code-block:: xml

    <configuration>
      <property>
        <name>hbase.coprocessor.user.region.classes</name>
        <value>org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor</value>
      </property>
    </configuration>

All new and existing non-system tables will have access to the GeoMesa Coprocessor.

Register Per-Table Through the HBase Shell
------------------------------------------

The HBase shell can be used to register coprocessors on a per-table basis, as shown below. Note that this requires
the tables to be offline.

When specifying a coprocessor, the coprocessor must be available on the HBase classpath on all of the master and
region servers or you must provide the HDFS URL for the ``geomesa-hbase-distributed-runtime`` JAR that was deployed
in :ref:`hbase_deploy_distributed_runtime`.

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

You will need to install the coprocessor on all the index tables. The ``geomesa`` table in this example is the metadata
table and does not need the coprocessor installed.

You can use the ``alter`` command to modify the configuration of the tables. The ``coprocessor`` parameter in the ``alter``
command may be modified to change the registration of the GeoMesa coprocessors.

.. code-block:: bash

    'coprocessor'=>'HDFS_URL|org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor|PRIORITY|'

The 'value' of the ``coprocessor`` parameter has four parts, separated by ``|``, two of which, ``HDFS_URL`` and
``PRIORITY``, are configurable depending on your environment.

 * To provide the HDFS URL of the ``geomesa-hbase-distributed-runtime`` JAR replace HDFS_URL in the coprocessor
   value with the HDFS URL. This is only need if the ``geomesa-hbase-distributed-runtime`` JAR will not be on the
   classpath by other means.
 * To alter the priority (execution order) of the coprocessor change PRIORITY to the desired value, this is optional
   and should be left blank if not used.

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

Register Per-Table Via Classpath
--------------------------------

If the ``geomesa-hbase-distributed-runtime`` JAR is on the HBase classpath for the master and all region servers,
it will automatically be registered when a GeoMesa table is created. To put the JAR on the classpath, modify the
``hbase-env.sh`` file on each node and add the path to the ``geomesa-hbase-distributed-runtime`` JAR in the
``HBASE_CLASSPATH`` property.

Register Per-Table Via GeoMesa Configuration
--------------------------------------------

If the GeoMesa environment is configured correctly, then the coprocessors will be registered when a GeoMesa table
is created. The ``geomesa-hbase-distributed-runtime`` JAR must be be accessible to the master and all region servers,
which typically means it should be located in HDFS or S3.

The path to the JAR can be configured via system property, or directly as a data store parameter.

If using a data store directly, the JAR path can be set with the datastore parameter ``coprocessor.url``.

In any environment, the JAR path can be set via the Java system property ``geomesa.hbase.coprocessor.path``. If
using the GeoMesa command-line tools, this may be set in the shell environment using the ``JAVA_TOOL_OPTIONS``
environment variable:

.. code-block:: bash

    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS} -Dgeomesa.hbase.coprocessor.path=hdfs://path/to/geomesa-runtime.jar"

Alternatively, it may be set in the ``geomesa-env.sh`` script:

.. code-block:: bash

    setvar CUSTOM_JAVA_OPTS "${JAVA_OPTS} -Dgeomesa.hbase.coprocessor.path=hdfs://path/to/geomesa-runtime.jar"

A third option is to use the ``geomesa-site.xml`` configuration file:

.. code-block:: xml

    <property>
        <name>geomesa.hbase.coprocessor.path</name>
        <value>hdfs://path/to/geomesa-runtime.jar</value>
    </property>
