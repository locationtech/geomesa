/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.geotools

import org.locationtech.jts.geom.Coordinate
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoToolsSpatialRDDProviderTest extends Specification {

  var sc: SparkContext = _

  step {
    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    sc = SparkContext.getOrCreate(conf)
  }

  val dsParams = Map("cqengine" -> "true", "geotools" -> "true")

  "The GeoToolsSpatialRDDProvider" should {
    "read from the in-memory database" in {
      val ds = DataStoreFinder.getDataStore(dsParams)
      ingestChicago(ds)

      val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), sc, dsParams, new Query("chicago"))
      rdd.count() mustEqual 3l
    }

    "write to the in-memory database" in {
      val ds = DataStoreFinder.getDataStore(dsParams)
      ds.createSchema(chicagoSft)
      val writeRdd = sc.parallelize(chicagoFeatures())
      GeoMesaSpark(dsParams).save(writeRdd, dsParams, "chicago")
      // verify write
      val readRdd = GeoMesaSpark(dsParams).rdd(new Configuration(), sc, dsParams, new Query("chicago"))
      readRdd.count() mustEqual 6l
    }
  }
  val chicagoSft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  def chicagoFeatures(): List[SimpleFeature] = {
    val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)
    val features = List(
      ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", createPoint(new Coordinate(-76.5, 38.5))),
      ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", createPoint(new Coordinate(-77.0, 38.0))),
      ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", createPoint(new Coordinate(-78.0, 39.0))),
      ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", createPoint(new Coordinate(-73.5, 39.5))),
      ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", createPoint(new Coordinate(-74.0, 35.5))),
      ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", createPoint(new Coordinate(-79.0, 37.5)))
    )
    features.foreach(_.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE))
    features
  }

  def ingestChicago(ds: DataStore): Unit = {
    val sft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")
    ds.createSchema(sft)

    val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

    val features = DataUtilities.collection(chicagoFeatures().take(3))

    fs.addFeatures(features)
  }
}