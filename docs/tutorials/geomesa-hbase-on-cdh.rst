Deploying GeoMesa HBase on Cloudera CDH
=======================================

- Download `GeoMesa HBase 1.3.X <https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-1.3.5/geomesa-hbase_2.11-1.3.5-bin.tar.gz>`_ or build from source.

- Unpack, and add/modify GeoMesa env variables in config ``../geomesa-hbase/conf/geomesa-env.sh`` :

.. code-block:: shell
    
    setvar HADOOP_HOME /opt/cloudera/parcels/CDH/lib/hadoop
    setvar HADOOP_CONF_DIR /etc/hadoop/conf
    
    hadoopCDH="1"
    
    setvar HADOOP_COMMON_HOME /opt/cloudera/parcels/CDH/lib/hadoop
    setvar HADOOP_HDFS_HOME /opt/cloudera/parcels/CDH/lib/hadoop-hdfs
    setvar YARN_HOME /opt/cloudera/parcels/CDH/lib/hadoop-yarn
    setvar HADOOP_MAPRED_HOME /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce

    setvar ZOOKEEPER_HOME /opt/cloudera/parcels/CDH/lib/zookeeper

- Upload ``../geomesa-hbase/dist/hbase/geomesa-hbase-distributed-runtime_2.11-1.3.X.jar`` to HDFS under ``hdfs:///hbase/lib``

- Create ``geomesa-site.xml`` under ``../geomesa-hbase/conf`` and add (change ``<name_node>`` to your HDFS name node hostname, and set your GeoMesa version e.g ``2.11-1.3.5``):

.. code-block:: xml
    
    <?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

    <configuration>
    <property>
        <name>geomesa.hbase.coprocessor.path</name>
        <value>hdfs://<name_node>:8020/hbase/lib/geomesa-hbase-distributed-runtime_2.11-1.3.X.jar</value>
        <description>HDFS or local path to GeoMesa-HBase Coprocessor JAR. If a local path is provided it must be the same for all region server. A path provided through the DataStore parameters will always override this      property.
        </description>
        <final>false</final>
    </property>
    </configuration>

- Symlink ``hbase-site.xml`` to GeoMesa conf dir.: ``ln -s /etc/hbase/conf.cloudera.hbase/hbase-site.xml ../geomesa-hbase/conf/hbase-site.xml``

- Run ``install-hbase.sh`` script: ``../geomesa-hbase/bin/./install-hbase.sh /path/to/geomesa-hbase_2.11-1.3.X/lib -h 1.2.3``

- Add additional jars from CDHs HBase to GeoMesa Classpath:

.. code-block:: shell
    
    ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/metrics-core-2.2.0.jar /path/to/geomesa-hbase_2.11-1.3.X/lib/metrics-core-2.2.0.jar;
    ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar /path/to/geomesa-hbase_2.11-1.3.X/lib/htrace-core-3.2.0-incubating.jar;

- All Set! Test client tools:

``bin/geomesa-hbase ingest -c example-csv -s example-csv -C example-csv examples/ingest/csv/example.csv``


