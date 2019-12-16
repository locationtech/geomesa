/***********************************************************************
  * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  ***********************************************************************/

package org.locationtech.geomesa.spark.cassandra

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.Query
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}


object CassandraRddService {


  def main(args: Array[String]): Unit = {

    var spark: SparkSession = null;
    var df: DataFrame = null;
    var sc: SQLContext = null;
    spark = createSparkSession()
    sc = spark.sqlContext


    val params = Map("cassandra.contact.point" -> "10.196.85.96:9042",
    "cassandra.keyspace" -> "gsc_tmo",
      "cassandra.catalog" -> "gsc",
      "cassandra.input.address" -> "10.196.85.96",
      "cassandra.input.partitioner" -> "Murmur3Partitioner")
    val query = new Query("raw_drops")

    val par =  new java.util.HashMap[String, String]()
    par.put("cassandra.contact.point","10.196.85.96:9042")
    par.put("cassandra.keyspace","gsc_tmo")
    par.put("cassandra.catalog","gsc")

    val rdd = GeoMesaSpark(par).rdd(new Configuration(), spark.sparkContext, params, query)

    //val sp = new CassandraSpatialRDDProvider()
   // val rdd = sp.rdd(new Configuration(), spark.sparkContext, params, query)

    println("Rdd First ________________" + rdd.first());

    //Save the RDD
    //sp.save(rdd,params1,"raw_drops")

  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()
  }

}
