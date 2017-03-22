/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.spark.sql.SparkSession
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.interop.WKTUtils

import scala.collection.JavaConversions._

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

  val ChiSpec = "arrest:String,case_number:Int:index=full:cardinality=high,dtg:Date,*geom:Point:srid=4326"
  val ChicagoSpec = SimpleFeatureTypes.createType("chicago", ChiSpec)

  def ingestChicago(ds: DataStore): Unit = {
    val sft = ChicagoSpec

    // Chicago data ingest
    ds.createSchema(sft)

    val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

    val parseDate = ISODateTimeFormat.basicDateTime().parseDateTime _
    val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

    val f = List(
      new ScalaSimpleFeature("1", sft, initialValues = Array("true","1",parseDate("20160101T000000.000Z").toDate, createPoint(new Coordinate(-76.5, 38.5)))),
      new ScalaSimpleFeature("2", sft, initialValues = Array("true","2",parseDate("20160102T000000.000Z").toDate, createPoint(new Coordinate(-77.0, 38.0)))),
      new ScalaSimpleFeature("3", sft, initialValues = Array("true","3",parseDate("20160103T000000.000Z").toDate, createPoint(new Coordinate(-78.0, 39.0))))
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
      new ScalaSimpleFeature(x._1, sft,
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
      new ScalaSimpleFeature(x._1, sft,
        initialValues=Array(x._1, WKTUtils.read(x._2)))
    }).toList)

    val fs = ds.getFeatureSource(name).asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(features)
  }
}

