/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.geotools.data.{DataStoreFinder, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLWithoutSedonaIT extends Specification {

  import scala.collection.JavaConverters._

  val dsParams = Map("cqengine" -> "true", "geotools" -> "true")

  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  var sc: SparkContext = _
  var spark: SparkSession = _

  step {
    spark = SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.ui.enabled", value = false)
      .master("local[2]")
      .getOrCreate()
    val ds = DataStoreFinder.getDataStore(dsParams.asJava)
    ds.createSchema(chicagoSft)
    WithClose(ds.getFeatureWriterAppend("chicago", Transaction.AUTO_COMMIT)) { writer =>
      chicagoFeatures.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  // This test is for verifying that GeoMesa SparkSQL could work without Apache Sedona: we should only load
  // Apache Sedona classes when spark.geomesa.use.sedona was set to true.
  "GeoMesa Spark SQL" should {
    "not have or using sedona" in {
      haveSedona must beFalse
      isUsingSedona must beFalse
    }

    "work well without Apache Sedona" in {
      val df = spark.read.format("geomesa").options(dsParams).option("geomesa.feature", "chicago").load()
      df.createOrReplaceTempView("chicago")
      val resDf = spark.sql(
        """
          |SELECT '7' as __fid__, arrest, case_number, dtg, st_castToPoint(st_translate(geom, 70, -30)) AS geom
          | FROM chicago
          | WHERE st_contains(st_makeBBOX(-76, 35, -73, 40), geom) AND CONCAT(__fid__, 'id') = '4id'""".stripMargin)
      val resList = resDf.collect()
      resList.length mustEqual 1L
      resList(0).getAs[Int]("case_number") mustEqual 4
      val resGeom = resList(0).getAs[Point]("geom")
      resGeom.getX mustEqual -3.5
      resGeom.getY mustEqual 9.5
    }
  }
}
