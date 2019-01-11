/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.apache.spark.sql.SparkSession
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.interop.WKTUtils

import scala.collection.JavaConversions._
import scala.util.Random

object SparkSQLTestUtils {
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()
  }

  val random = new Random()
  random.setSeed(0)

  val ChiSpec = "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326"
  val ChicagoSpec = SimpleFeatureTypes.createType("chicago", ChiSpec)

  def ingestChicago(ds: DataStore): Unit = {
    val sft = ChicagoSpec

    // Chicago data ingest
    ds.createSchema(sft)

    val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

    val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

    val f = List(
      ScalaSimpleFeature.create(sft, "1", "true", "1", "2016-01-01T00:00:00.000Z", createPoint(new Coordinate(-76.5, 38.5))),
      ScalaSimpleFeature.create(sft, "2", "true", "2", "2016-01-02T00:00:00.000Z", createPoint(new Coordinate(-77.0, 38.0))),
      ScalaSimpleFeature.create(sft, "3", "true", "3", "2016-01-03T00:00:00.000Z", createPoint(new Coordinate(-78.0, 39.0)))
    )

    f.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))

    fs.addFeatures(DataUtilities.collection(f))
  }

  def ingestPoints(ds: DataStore,
                   name: String,
                   points: Map[String, String]): Unit = {
    val sft = SimpleFeatureTypes.createType(
      name, "name:String,*geom:Point:srid=4326")
    ds.createSchema(sft)

    val features = DataUtilities.collection(points.map(x => {
      new ScalaSimpleFeature(sft, x._1,
        initialValues=Array(x._1, WKTUtils.read(x._2).asInstanceOf[Point]))
    }).toList)

    val fs = ds.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(features)
  }

  def ingestGeometries(ds: DataStore,
                       name: String,
                       geoms: Map[String, String]): Unit = {
    val sft = SimpleFeatureTypes.createType(
      name, "name:String,*geom:Geometry:srid=4326")
    ds.createSchema(sft)

    val features = DataUtilities.collection(geoms.map(x => {
      new ScalaSimpleFeature(sft, x._1,
        initialValues=Array(x._1, WKTUtils.read(x._2)))
    }).toList)

    val fs = ds.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(features)
  }

  def generatePoints(gf: GeometryFactory, numPoints: Int): Map[String, String] = {
    (1 until numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      (i.toString, gf.createPoint(new Coordinate(x, y)).toText)
    }.toMap
  }

  def generatePolys(gf: GeometryFactory, numPoints: Int): Map[String, String] = {
    (1 until numPoints).map { i =>
      val x = -180 + 360 * random.nextDouble()
      val y = -90 + 180 * random.nextDouble()
      val width = (3 * random.nextDouble()) / 2.0
      val height = (1 * random.nextDouble()) / 2.0
      val (minX, maxX, minY, maxY) = (x - width, x + width, y - height, y + height)
      val coords = Array(new Coordinate(minX, minY), new Coordinate(minX, maxY),
        new Coordinate(maxX, minY), new Coordinate(maxX, maxY), new Coordinate(minX, minY))
      (i.toString, gf.createPolygon(coords).toText)
    }.toMap
  }
}

