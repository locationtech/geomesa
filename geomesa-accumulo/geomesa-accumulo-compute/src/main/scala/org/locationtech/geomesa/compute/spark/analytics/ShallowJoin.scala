/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.compute.spark.analytics

import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.geotools.data.DataStoreFinder
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.opengis.feature.simple.SimpleFeature
import org.apache.hadoop.conf.Configuration
import org.geotools.data._

object ShallowJoin {
  val countriesDsParams = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1,zoo2,zoo3",
    "user"       -> "user",
    "password"   -> "password",
    "tableName"  -> "countries")

  val gdeltDsParams = Map(
    "instanceId" -> "mycloud",
    "zookeepers" -> "zoo1,zoo2,zoo3",
    "user"       -> "user",
    "password"   -> "password",
    "tableName"  -> "gdelt")

  val countriesDs = DataStoreFinder.getDataStore(countriesDsParams).asInstanceOf[AccumuloDataStore]
  val gdeltDs = DataStoreFinder.getDataStore(gdeltDsParams).asInstanceOf[AccumuloDataStore]

  def main(args: Array[String]) {

    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), countriesDs))
    GeoMesaSpark.register(gdeltDs)

    val countriesRdd: RDD[SimpleFeature] = GeoMesaSpark.rdd(new Configuration(), sc, countriesDsParams, new Query("states"))
    val gdeltRdd: RDD[SimpleFeature] = GeoMesaSpark.rdd(new Configuration(), sc, gdeltDsParams, new Query("gdelt"))

    val aggregated = GeoMesaSpark.shallowJoin(sc, countriesRdd, gdeltRdd, "STATE_NAME")

    aggregated.collect.foreach {println}

    countriesDs.dispose()
    gdeltDs.dispose()

  }
}
