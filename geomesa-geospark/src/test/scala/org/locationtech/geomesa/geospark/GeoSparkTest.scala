/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import scala.collection.JavaConversions._

import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.datasyslab.geospark.spatialOperator.{KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.jts.TestEnvironment
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoSparkTest extends Specification with TestEnvironment
                                         with GeoMesaSparkTestEnvironment
                                         with GeoSparkTestEnvironment {

  //  TestEnvironment used to parse CSV's into DataFrames
  //  GeoMesaSparkTestEnvironment is used for creating RDDs in a GeoMesaSpark context
  //  GeoSparkTestEnvironment used for managing data in GeoSpark

  sequential

  var df: DataFrame = _
  var newDF: DataFrame = _

  // Convert SimpleFeature to Point
  val simpleFeatureToPoint = (sf: SimpleFeature)  => {
      val pt = sf.getAttribute("point").asInstanceOf[Point]
      pt.setUserData(sf)
      pt
  }

  // Convert SimpleFeature to Polygon
  val simpleFeatureToPolygon = (sf: SimpleFeature)  => {
    val poly = sf.getAttribute("polygon").asInstanceOf[Polygon]
    poly.setUserData(sf)
    poly
  }

  // Convert SimpleFeature to LongLat
  val simpleFeatureToLongLat = (sf: SimpleFeature)  => {
    val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
    val lat = sf.getAttribute("latitude").asInstanceOf[Double]
    val long = sf.getAttribute("longitude").asInstanceOf[Double]
    val pt = geometryFactory.createPoint(new Coordinate(long, lat))
    pt.setUserData(sf)
    pt
  }

  // before
  step {
    val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
    df = spark.read.format("csv")
      .option("delimiter", "\n")
      .option("header", "false")
      .option("sep", "-")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").load(dataFile)
    df.createOrReplaceTempView("inputtable")
    df.show()
  }

  val dsParams = Map("cqengine" -> "true", "geotools" -> "true")

  lazy val jtsExampleSft =
    SimpleFeatureTypes.createType("jtsExample",
    "*point:Point:srid=4326,*polygon:Polygon:srid=4326,latitude:Double,longitude:Double")

  // initialize sample geometries
  lazy val jtsExampleFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(jtsExampleSft, "itemA", "POINT(40 40)", "POLYGON((35 35, 45 35, 45 45, 35 45, 35 35))", 40, 40),
    ScalaSimpleFeature.create(jtsExampleSft, "itemB", "POINT(30 30)", "POLYGON((25 25, 35 25, 35 35, 25 35, 25 25))", 30, 30),
    ScalaSimpleFeature.create(jtsExampleSft, "itemC", "POINT(20 20)", "POLYGON((15 15, 25 15, 25 25, 15 25, 15 15))", 20, 20)
  )

  "geomesa rdd" should {
    "read from dataframe" in {
      val geomesaRDD = df.rdd
      geomesaRDD.collect()
      geomesaRDD.count mustEqual(df.count)
    }
  }

  "simplefeatures" should {
      "should convert to point rdd" in {
        val ds = DataStoreFinder.getDataStore(dsParams)
        ds.createSchema(jtsExampleSft)

        WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
          jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
        val pointRDD = rdd.map(simpleFeatureToPoint)
        pointRDD.collect().foreach(println)

        var isPointRDD = true
        pointRDD.collect().foreach(pt => isPointRDD = (isPointRDD && pt.isInstanceOf[Point]))
        isPointRDD mustEqual(true)
      }
      "should convert to polygon rdd" in {
        val ds = DataStoreFinder.getDataStore(dsParams)
        ds.createSchema(jtsExampleSft)

        WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
          jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
        val polygonRDD = rdd.map(simpleFeatureToPolygon)
        polygonRDD.collect().foreach(println)

        var isPolygonRDD = true
        polygonRDD.collect().foreach(poly => isPolygonRDD = (isPolygonRDD && poly.isInstanceOf[Polygon]))
        isPolygonRDD mustEqual(true)
      }
      "should convert to long/lat rdd" in {
        val ds = DataStoreFinder.getDataStore(dsParams)
        ds.createSchema(jtsExampleSft)

        WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
          jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }

        val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
        val pointRDD = rdd.map(simpleFeatureToLongLat)
        pointRDD.collect().foreach(println)

        var isPointRDD = true
        pointRDD.collect().foreach(pt => isPointRDD = (isPointRDD && pt.isInstanceOf[Point]))
        isPointRDD mustEqual(true)
      }
    }

  "geomesaspark rdd" should {
    "convert to geospark rdd" >> {
      val ds = DataStoreFinder.getDataStore(dsParams)
      ds.createSchema(jtsExampleSft)

      WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
        jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      //  Create GeoMesaSpark Point RDD from SimpleFeatures
      val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
      val pointRDD = rdd.map(simpleFeatureToPoint)
      //  Create GeoSpark Point RDD from GeoMesaSpark Point RDD
      val gsPointRDD = new PointRDD(pointRDD)
      true mustEqual(true)
    }
  }

  // after
  step {
    spark.stop()
  }
}
