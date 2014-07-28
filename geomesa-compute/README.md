<p align="center"><a href="http://geomesa.github.io"><img src="https://raw.githubusercontent.com/geomesa/geomesa.github.io/master/img/geomesa-2x.png"></img></a></p>

## GeoMesa Compute

Run distributed spatial computations and analytics on your geospatial
data using Apache Spark.  To instantiate a ```RDD[SimpleFeature]```,
call ```GeoMesaSpark.init(sparkConf, ds)``` on one of your data stores.
Then, request a ```RDD``` using a CQL filter as follows:

```scala
geomesa.compute.spark.GeoMesaSpark.rdd(hadoopConf, sparkConf, ds, query)
```

The following example demonstrates computing a time series across
geospatial data within the constraints of a CQL query.

```scala
object CountByDay {
  def main(args: Array[String]) {
    val params = Map(
      "instanceId" -> "instance",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> "user",
      "password"   -> "*****",
      "auths"      -> "USER,ADMIN",
      "tableName"  -> "geomesa_catalog")

    val ds = DataStoreFinder.getDataStore(params)

    val ff = CommonFactoryFinder.getFilterFactory2
    val f = ff.bbox("geom", -80, 35, -79, 36, "EPSG:4326")
    val q = new Query("myFeatureType", f)
    val conf = new Configuration

    val sconf = init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    countByDay(conf, sc, ds.asInstanceOf[AccumuloDataStore], q).collect().foreach(println)
  }
}
```

To run the example, execute the following command:

```shell
$ /opt/spark/bin/spark-submit --master yarn-client --num-executors 40 --executor-cores 4  countbyday.jar --deploy-mode client --class com.mycompany.example.CountByDay
```
