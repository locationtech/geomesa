# GeoMesa-fs (filesystem backend parquet on HDFS)

## Client tools IMPORT

`./geomesa-fs ingest --path hdfs://1.sherpa.client.sysedata.no:8020/DATASETS/AIS/dkaisParquet --encoding parquet --spec ../../geomesa_conf/dkais.sft --converter ../../geomesa_conf/dkais.convert --partition-scheme hourly,z2-4bit /STAGING/DATASETS/AIS/tmp/aisdk_20160401.csv`


## Client tools EXPORT

`./geomesa-fs export --encoding parquet --feature-name dkais --path hdfs://sherpa1:8020/DATASETS/AIS/dkaisParquet --max-features 100`

## NIFI import
* Todo

## Spark 

* Spark SQL query:
```
spark-shell --jars /home/kentt/repos/geomesa/geomesa-fs/geomesa-fs-spark-runtime/target/geomesa-fs-spark-runtime_2.11-1.3.5.jar

val dataFrame = spark.read.format("geomesa").option("fs.encoding","parquet").option("fs.path","///DATASETS/AIS/dkaisParquet").option("geomesa.feature","dkais").load()

dataFrame.createOrReplaceTempView("dkais")

spark.sql("SELECT * FROM dkais WHERE st_contains(st_makeBBOX(8.4, 54.0, 8.6, 55.0), geom)").show() 
+---------+--------------------+---------+---+-----+--------------------+
|  __fid__|             aistime|     mmsi|sog|  cog|                geom|
+---------+--------------------+---------+---+-----+--------------------+
|211222960|2016-04-01 20:00:...|211222960|0.0|249.1|POINT (8.576295 5...|
|211222960|2016-04-01 20:00:...|211222960|0.0|249.1|POINT (8.576303 5...|
|211222960|2016-04-01 20:00:...|211222960|0.0|249.1|POINT (8.576308 5...|
|211222960|2016-04-01 20:00:...|211222960|0.0|249.1|POINT (8.576303 5...|
|211222960|2016-04-01 20:00:...|211222960|0.0|249.1|POINT (8.576298 5...|
.
.
.


```

NB! Spark needs to be < 2.2.0

* PySpark SQL query: 
```
/share/hadoop_custom/spark-2.0.0/bin/./pyspark --jars /home/kentt/repos/geomesa/geomesa-fs/geomesa-fs-spark-runtime/target/geomesa-fs-spark-runtime_2.11-1.3.5.jar

same as for scala to produce the example
```

* Register as Hive table and query from Hue (GeoMesa side of things not working):
(Guide) [https://community.hortonworks.com/questions/32810/spark-temporary-table-is-not-shown-in-beeline.html]
```
/share/hadoop_custom/spark-2.0.0/bin/./spark-shell --jars /home/kentt/repos/geomesa/geomesa-fs/geomesa-fs-spark-runtime/target/geomesa-fs-spark-runtime_2.11-1.3.5.jar --conf spark.sql.hive.thriftServer.singleSession=true

import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)

val dataFrame = spark.read.format("geomesa").option("fs.encoding","parquet").option("fs.path","///DATASETS/AIS/dkaisParquet").option("geomesa.feature","dkais").load()
dataFrame.createOrReplaceTempView("dkais")

hiveContext.setConf("hive.server2.thrift.port","11123")
HiveThriftServer2.startWithContext(hiveContext)
```

From Beeline shell:
```
beeline
!connect jdbc:hive2://localhost:11123

SELECT * mmsi FROM dkais LIMIT 10;
+------------+--+
|    mmsi    |
+------------+--+
| 219849000  |
| 375448000  |
| 256812000  |
| 2190064    |
| 311794000  |
| 2194005    |
| 2190045    |
| 304352000  |
| 219000742  |
| 220338000  |
+------------+--+

SELECT * FROM dkais WHERE st_contains(st_makeBBOX(8.4, 54.0, 8.6, 55.0), geom)
Error: scala.MatchError: org.apache.spark.sql.PointUDT@628e0403 (of class org.apache.spark.sql.PointUDT) (state=,code=0)
```
Note: Spark UDFs not recognized

## GeoServer

* Hadoop Jar dependencies: 
```
hadoop-annotations.jar
hadoop-auth.jar
hadoop-common.jar
hadoop-mapreduce-client-core.jar
hadoop-yarn-api.jar
hadoop-yarn-common.jar
commons-configuration.jar

commons-cli-1.2.jar
hadoop-client-2.6.0-cdh5.13.0.jar
hadoop-hdfs.jar
htrace-core4-4.0.1-incubating.jar
```

