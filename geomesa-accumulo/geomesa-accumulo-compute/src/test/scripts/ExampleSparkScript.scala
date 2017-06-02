/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.compute.spark.GeoMesaSpark

import scala.collection.JavaConversions._

//
// This script is meant to be copy-pasted into spark shell
// to test out our classloading
//

// Set these
val params = Map(
  "instanceId" -> "local",
  "zookeepers" -> "localhost",
  "user"       -> "USER",
  "password"   -> "PASSWORD",
  "tableName"  -> "SOURCE_TABLE")

val params2 =  Map(
  "instanceId" -> "local",
  "zookeepers" -> "localhost",
  "user"       -> "USER",
  "password"   -> "PASSWORD",
  "tableName"  -> "TARGET_TABLE")

val typeName = "TYPENAME HERE"


//
// Execute some code to use the input and output RDD
//
val ds = DataStoreFinder.getDataStore(params)

// Call initialize, retrieving a new spark config
val newConfig = GeoMesaSpark.init(new SparkConf(), ds)
// Stop all other running Spark Contexts
Option(sc).foreach(_.stop())
// Initialize a new one with the desired config
var sc = new SparkContext(newConfig)

val filter = "INTERSECTS(geom, POLYGON((-86.0229 37.5272, -74.3774 37.5272, -74.3774 33.7426, -86.0229 33.7426, -86.0229 37.5272)))"

val q = new Query(typeName, ECQL.toFilter(filter))

val queryRDD = GeoMesaSpark.rdd(new Configuration, sc, params, q, None)
val dayAndFeature = queryRDD.mapPartitions { iter =>
  val df = new SimpleDateFormat("yyyyMMdd")
  val ff = CommonFactoryFinder.getFilterFactory2
  val exp = ff.property("dtg")
  iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
}

val countByDay = dayAndFeature.map( x => (x._1, 1)).reduceByKey(_ + _)
countByDay.collect().foreach(println)

val sft = ds.getSchema(typeName)

val ds2 = DataStoreFinder.getDataStore(params2)
ds2.createSchema(sft)
GeoMesaSpark.save(queryRDD, params2, typeName)

// Call initialize, retrieving a new spark config
val newConfig2 = GeoMesaSpark.init(new SparkConf(), ds2)
// Stop all other running Spark Contexts
Option(sc).foreach(_.stop())
// Initialize a new one with the desired config
sc = new SparkContext(newConfig2)

val filter2 = "INTERSECTS(geom, POLYGON((-86.0229 37.5272, -74.3774 37.5272, -74.3774 33.7426, -86.0229 33.7426, -86.0229 37.5272)))"

val q2 = new Query(typeName, ECQL.toFilter(filter2))

val queryRDD2 = GeoMesaSpark.rdd(new Configuration, sc, params2, q2, None)
val dayAndFeature2 = queryRDD2.mapPartitions { iter =>
  val df = new SimpleDateFormat("yyyyMMdd")
  val ff = CommonFactoryFinder.getFilterFactory2
  val exp = ff.property("dtg")
  iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
}
val countByDay2 = dayAndFeature2.map( x => (x._1, 1)).reduceByKey(_ + _)
countByDay2.collect().foreach(println)