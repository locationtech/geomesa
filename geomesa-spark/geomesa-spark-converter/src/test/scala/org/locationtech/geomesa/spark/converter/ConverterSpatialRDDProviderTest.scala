/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.converter

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ConverterSpatialRDDProviderTest extends Specification {

  sequential

  import ConverterSpatialRDDProvider.{ConverterKey, IngestTypeKey, InputFilesKey, SftKey}

  var sc: SparkContext = null

  step {
    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    sc = SparkContext.getOrCreate(conf)
  }

  val exampleConf = ConfigFactory.load()
  val converterConf = exampleConf.getConfig("geomesa.converters.example-csv")

  val params = Map(
    InputFilesKey -> getClass.getResource("/example.csv").getPath,
    ConverterKey  -> converterConf.root().render(ConfigRenderOptions.concise()),
    SftKey        -> exampleConf.root().render(ConfigRenderOptions.concise())
  )

  "The ConverterSpatialRDDProvider" should {
    "read from local files" in {
      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, new Query("example-csv"))
      rdd.count() mustEqual 3l
      rdd.collect.map(_.getAttribute("name").asInstanceOf[String]).toList must
          containTheSameElementsAs(Seq("Harry", "Hermione", "Severus"))
    }

    "read from local files with filtering" in {
      val query = new Query("example-csv", ECQL.toFilter("name like 'H%'"))
      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)
      rdd.count() mustEqual 2l
      rdd.collect.map(_.getAttribute("name").asInstanceOf[String]).toList must
          containTheSameElementsAs(Seq("Harry", "Hermione"))
    }

    "read from a local file using Converter Name lookup" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )

      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, new Query("example-csv"))
      rdd.count() mustEqual 3l
    }

    "handle projections" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )
      val requestedProps : Array[String] = Array("name")
      val q = new Query("example-csv", Filter.INCLUDE, requestedProps)
      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, q)
      val returnedProps = rdd.first.getProperties.map{_.getName.toString}.toArray
      returnedProps mustEqual requestedProps
    }

    "handle * projections" in {
      val params = Map (
        InputFilesKey -> getClass.getResource("/example.csv").getPath,
        IngestTypeKey -> "example-csv"
      )
      val requestedProps : Array[String] = Array( "fid", "name", "age", "lastseen", "friends","talents", "geom")
      val q = new Query("example-csv", Filter.INCLUDE, requestedProps)
      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, q)
      val returnedProps = rdd.first.getProperties.map{_.getName.toString}.toArray
      returnedProps mustEqual requestedProps
    }
  }
}
