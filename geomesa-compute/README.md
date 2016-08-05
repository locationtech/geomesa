<p align="center"><a href="http://geomesa.github.io"><img src="https://raw.githubusercontent.com/geomesa/geomesa.github.io/master/img/geomesa-2x.png"></img></a></p>

## GeoMesa Compute

Run distributed spatial computations and analytics on your geospatial
data using Apache Spark.  To instantiate a ```RDD[SimpleFeature]```,
call ```GeoMesaSpark.init(sparkConf, ds)``` on one of your data stores.
Then, request a ```RDD``` using a CQL filter as follows:

```scala
geomesa.compute.spark.GeoMesaSpark.rdd(hadoopConf, sparkContext, ds, query)
```

The following example demonstrates computing a time series across
geospatial data within the constraints of a CQL query.

```scala
object CountByDay {
  def main(args: Array[String]) {
    // Get a handle to the data store
    val params = Map(
      "instanceId" -> "instance",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> "user",
      "password"   -> "*****",
      "auths"      -> "USER,ADMIN",
      "tableName"  -> "geomesa_catalog")

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    // Construct a CQL query to filter by bounding box
    val ff = CommonFactoryFinder.getFilterFactory2
    val f = ff.bbox("geom", -80, 35, -79, 36, "EPSG:4326")
    val q = new Query("myFeatureType", f)
    
    // Configure Spark    
    val conf = new Configuration
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    // Create an RDD from a query
    val queryRDD = GeoMesaSpark.rdd(conf, sc, ds, query)
    
    // Convert RDD[SimpleFeature] to RDD[(String, SimpleFeature)] where the first
    // element of the tuple is the date to the day resolution
    val dayAndFeature = queryRDD.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property("dtg")
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }
    
    // Count the number of features in each day
    val countByDay = dayAndFeature.map( x => (x._1, 1)).reduceByKey(_ + _) 
    
    // Collect the results and print
    countByDay.collect.foreach(println)
  }

}
```

To run the example, execute the following command:

```shell
$ /opt/spark/bin/spark-submit --master yarn-client --num-executors 40 --executor-cores 4  countbyday.jar --deploy-mode client --class com.mycompany.example.CountByDay
```

### Spark Shell Execution

Add the following values to `$SPARK_HOME/conf/spark-defaults.conf`:

```
spark.serializer                   org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator             org.locationtech.geomesa.compute.spark.GeoMesaSparkKryoRegistrator
spark.kryo.registrationRequired    false
```

To run the spark shell (for spark version 2.0.0) compile and run:

    bin/spark-shell --jars /path/to/geomesa-compute-1.2.4-shaded.jar

The example code above is provided a script for `spark-shell` under `geomesa-compute/scripts/test/scala/localtest.scala`.

