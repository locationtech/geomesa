/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Run with:
// bin/spark-shell --driver-class-path /path/to/geomesa-compute-accumulo1.5-1.0.0-rc.2-SNAPSHOT-shaded.jar
// copy paste all this into the spark shell version 1.1.0

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.accumulo.data._
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

// feature name in GeoMesa to query on
val feature = "featname"

// Get a handle to the data store
val params = Map(
  "instanceId" -> "inst",
  "zookeepers" -> "zoo1,zoo2,zoo3",
  "user"       -> "user",
  "password"   -> "pass",
  "tableName"  -> "geomesa_catalog")
val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

// We'll grab everything...but usually you want some CQL filter here (e.g. bbox)
val q = new Query(feature, Filter.INCLUDE)

// Configure Spark to run locally with 4 threads
val conf = new Configuration
val sconf = GeoMesaSpark.init(new SparkConf(true).setAppName("localtest").setMaster("local[4]"), ds)
val sc = new SparkContext(sconf)

// Create an RDD from a query
val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, params, q)

// Convert RDD[SimpleFeature] to RDD[(String, SimpleFeature)] where the first
// element of the tuple is the date to the day resolution
val dayAndFeature = queryRDD.mapPartitions { iter =>
  val df = new SimpleDateFormat("yyyyMMdd")
  val ff = CommonFactoryFinder.getFilterFactory2
  val exp = ff.property("dtg")
  iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
}

// Group the results by day
val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }
// Count the number of features in each day
val countByDay = groupedByDay.map { case (date, iter) => (date, iter.size) }
// Collect the results and print
countByDay.collect.foreach(println)