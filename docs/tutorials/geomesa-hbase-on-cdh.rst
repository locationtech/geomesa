Deploying GeoMesa HBase on Cloudera CDH 5.X
===========================================

.. warning::

  Ensure that the CDH version you are using contains a compatible version of HBase. As of this writing,
  the latest CDH release is 5.16.x containing HBase 1.2, which is no longer supported by GeoMesa.

- Download and extract the GeoMesa HBase distribution, as detailed in :ref:`setting_up_hbase_commandline`. In the
  following steps, ``GEOMESA_HBASE_HOME`` refers to the extracted directory ``geomesa-hbase_{{scala_binary_version}}-{{release}}/``.

- Unpack, and add/modify GeoMesa environment variables in the files ``$GEOMESA_HBASE_HOME/conf/*-env.sh`` :

.. code-block:: shell
    
    setvar HADOOP_HOME /opt/cloudera/parcels/CDH/lib/hadoop
    setvar HADOOP_CONF_DIR /etc/hadoop/conf
    
    hadoopCDH="1"
    
    setvar HADOOP_COMMON_HOME /opt/cloudera/parcels/CDH/lib/hadoop
    setvar HADOOP_HDFS_HOME /opt/cloudera/parcels/CDH/lib/hadoop-hdfs
    setvar YARN_HOME /opt/cloudera/parcels/CDH/lib/hadoop-yarn
    setvar HADOOP_MAPRED_HOME /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce

    setvar ZOOKEEPER_HOME /opt/cloudera/parcels/CDH/lib/zookeeper

- Copy ``$GEOMESA_HBASE_HOME/dist/hbase/geomesa-hbase-distributed-runtime_{{scala_binary_version}}-{{release}}.jar`` to HDFS under ``hdfs:///hbase/lib``

- Create ``geomesa-site.xml`` under ``$GEOMESA_HBASE_HOME/conf`` and add (changing ``[name_node]`` to your HDFS name
  node hostname):

.. code-block:: xml
    
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

    <configuration>
    <property>
        <name>geomesa.hbase.coprocessor.path</name>
        <value>hdfs://[name_node]:8020/hbase/lib/geomesa-hbase-distributed-runtime_{{scala_binary_version}}-{{release}}.jar</value>
        <description>HDFS or local path to GeoMesa-HBase Coprocessor JAR. If a local path is provided it must be
          the same for all region servers. A path provided through the DataStore parameters will always
          override this property.
        </description>
        <final>false</final>
    </property>
    </configuration>

- Symlink ``hbase-site.xml`` to the GeoMesa conf dir:

.. code-block:: shell

    ln -s /etc/hbase/conf.cloudera.hbase/hbase-site.xml $GEOMESA_HBASE_HOME/conf/hbase-site.xml

- Modify ``$GEOMESA_HBASE_HOME/conf/dependencies.sh`` to set ``hbase_version`` to ``1.2.0`` at the top of the script.

- Run the ``install-dependencies.sh`` script, which will download JARs to the ``lib`` folder:

.. code-block:: shell

    $GEOMESA_HBASE_HOME/bin/install-dependencies.sh

- Add additional JARs from CDH HBase to the GeoMesa classpath:

.. code-block:: shell
    
    ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/metrics-core-2.2.0.jar \
      $GEOMESA_HBASE_HOME/lib/metrics-core-2.2.0.jar;
    ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar \
      $GEOMESA_HBASE_HOME/lib/htrace-core-3.2.0-incubating.jar;

- All Set! Test client tools by ingesting the provided example data:

.. code-block:: shell

  $GEOMESA_HBASE_HOME/bin/geomesa-hbase ingest -c example-csv -s example-csv \
    -C example-csv $GEOMESA_HBASE_HOME/examples/csv/example.csv
