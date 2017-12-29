# Deploying GeoMesa 1.3.x on CDH 5.X.X

## Quickstart
* Download [GeoMesa HBase 1.3.5](https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-1.3.5/geomesa-hbase_2.11-1.3.5-bin.tar.gz)

* Unpack, and add to config `../geomesa-hbase/conf/geomesa-env.sh`:
```
setvar HADOOP_HOME /opt/cloudera/parcels/CDH/lib/hadoop
setvar HADOOP_CONF_DIR /etc/hadoop/conf

hadoopCDH="1"

setvar HADOOP_COMMON_HOME /opt/cloudera/parcels/CDH/lib/hadoop
setvar HADOOP_HDFS_HOME /opt/cloudera/parcels/CDH/lib/hadoop-hdfs
setvar YARN_HOME /opt/cloudera/parcels/CDH/lib/hadoop-yarn
setvar HADOOP_MAPRED_HOME /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce

setvar ZOOKEEPER_HOME /opt/cloudera/parcels/CDH/lib/zookeeper
```
* Upload `../dist/hbase/geomesa-hbase-distributed-runtime_2.11-1.3.5.jar` to HDFS under `hdfs:///hbase/lib`
* Copy `geomesa-site.xml` to `../geomesa-hbase/conf`
* Symlink `hbase-site.xml`: `ln -s /etc/hbase/conf.cloudera.hbase/hbase-site.xml ../geomesa-hbase/conf/hbase-site.xml`
* Run `install-hbase.sh` script: `../geomesa-hbase/bin/./install-hbase.sh /path/to/geomesa-hbase_2.11-1.3.5/lib -h 1.2.3`
* Get missing jars from CDH HBase: 
```
ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/metrics-core-2.2.0.jar /path/to/geomesa-hbase_2.11-1.3.5/lib/metrics-core-2.2.0.jar;
ln -s /opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.2.0-incubating.jar /path/to/geomesa-hbase_2.11-1.3.5/lib/htrace-core-3.2.0-incubating.jar;
```
* All Set! Test client tools:
`bin/geomesa-hbase ingest -c example-csv -s example-csv -C example-csv examples/ingest/csv/example.csv`

## Build from source 
