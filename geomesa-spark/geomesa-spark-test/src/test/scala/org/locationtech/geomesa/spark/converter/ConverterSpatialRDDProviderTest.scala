/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.converter

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.geotools.api.data.Query
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.specs2.mutable.SpecificationWithJUnit

class ConverterSpatialRDDProviderTest extends SpecificationWithJUnit {

  import ConverterSpatialRDDProvider.{ConverterKey, IngestTypeKey, InputFilesKey, SftKey}
  import org.locationtech.geomesa.spark.jts._

  import scala.collection.JavaConverters._

  val exampleConf = ConfigFactory.load()
  val converterConf = exampleConf.getConfig("geomesa.converters.example-csv")

  val params = Map(
    InputFilesKey -> getClass.getResource("/example.csv").getPath,
    ConverterKey  -> converterConf.root().render(ConfigRenderOptions.concise()),
    SftKey        -> exampleConf.root().render(ConfigRenderOptions.concise())
  )

  // TODO the converter provider isn't bundled into a spark runtime, so doesn't work with distributed spark clusters
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", value = false)
      .master(s"local[1]")
      .getOrCreate()
      .withJTS
  }
  lazy val sc: SQLContext = spark.sqlContext
  lazy val sparkContext: SparkContext = spark.sparkContext

  "The ConverterSpatialRDDProvider" should {
    "read from local files" in {
      val rdd = GeoMesaSpark(params.asJava).rdd(new Configuration(), sparkContext, params, new Query("example-csv"))
      rdd.count() mustEqual 3l
      rdd.collect.map(_.getAttribute("name").asInstanceOf[String]).toList must
          containTheSameElementsAs(Seq("Harry", "Hermione", "Severus"))
    }

    "read from local files with filtering" in {
      val query = new Query("example-csv", ECQL.toFilter("name like 'H%'"))
      val rdd = GeoMesaSpark(params.asJava).rdd(new Configuration(), sparkContext, params, query)
      rdd.count() mustEqual 2l
      rdd.collect.map(_.getAttribute("name").asInstanceOf[String]).toList must
          containTheSameElementsAs(Seq("Harry", "Hermione"))
    }

    "read from a local file using Converter Name lookup" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )

      val rdd = GeoMesaSpark(params.asJava).rdd(new Configuration(), sparkContext, params, new Query("example-csv"))
      rdd.count() mustEqual 3l
    }

    "handle projections" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )
      val requestedProps: Array[String] = Array("name")
      val q = new Query("example-csv", Filter.INCLUDE, requestedProps: _*)
      val rdd = GeoMesaSpark(params.asJava).rdd(new Configuration(), sparkContext, params, q)
      val returnedProps = rdd.first.getProperties.asScala.map{_.getName.toString}.toArray
      returnedProps mustEqual requestedProps
    }

    "handle * projections" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )
      val requestedProps: Array[String] = Array( "fid", "name", "age", "lastseen", "friends","talents", "geom")
      val q = new Query("example-csv", Filter.INCLUDE, requestedProps: _*)
      val rdd = GeoMesaSpark(params.asJava).rdd(new Configuration(), sparkContext, params, q)
      val returnedProps = rdd.first.getProperties.asScala.map{_.getName.toString}.toArray
      returnedProps mustEqual requestedProps
    }
  }
}
