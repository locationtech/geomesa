/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}

trait GeoMesaSparkTestEnvironment {
  // The tests regarding this does not need a spark session - just use gmsc as the context

//  implicit lazy val gmsc: SparkSession = {
//    SparkSession.builder()
//      .appName("testGeoMesaSpark")
//      .master("local[2]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
//      .master("local[*]").appName("myGeoMesaSparkdemo").getOrCreate()
//  }

  var gmsc: SparkContext = _

  val gmconf = new SparkConf().setMaster("local[2]").setAppName("GeoMesaSparkRunnableExample")
  gmconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  gmconf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
  gmsc = SparkContext.getOrCreate(gmconf)
}
