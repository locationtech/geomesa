/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable

import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.bigtable.data.BigtableDataStoreFactory
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BigTableIntegrationTest extends Specification {

  // note: make sure you update src/test/resources/hbase-site.xml to point to your bigtable instance

  sequential

  "HBaseDataStore" should {
    "work with points" >> {
      val typeName = "testpoints"
      val params = Map(BigtableDataStoreFactory.BigtableCatalogParam.getName -> "integration_test")
      lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      def createFeatures(sft: SimpleFeatureType) = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name $i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      "create schema" >> {
        skipped("integration")

        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String,dtg:Date,*geom:Point:srid=4326"))
        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)
      }

      "insert" >> {
        skipped("integration")

        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)

        val features = createFeatures(sft)

        val fs = ds.getFeatureSource(typeName)
        val ids = fs.addFeatures(new ListFeatureCollection(sft, features))
        ids.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))
      }

      "query" >> {
        skipped("integration")

        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)

        val features = createFeatures(sft)

        testQuery(ds, typeName, "INCLUDE", null, features)
        testQuery(ds, typeName, "IN('0', '2')", null, Seq(features(0), features(2)))
        testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, features.dropRight(2))
        testQuery(ds, typeName, "bbox(geom,42,48,52,62)", null, features.drop(2))
      }
    }

    "work with points" >> {
      val typeName = "testpolys"
      val params = Map(HBaseCatalogParam.getName -> "integration_test")
      lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      def createFeatures(sft: SimpleFeatureType) = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name $i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
        sf.setAttribute(2, s"POLYGON((-120 4$i, -120 50, -125 50, -125 4$i, -120 4$i))")
        sf
      }

      "create schema" >> {
        skipped("integration")

        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String,dtg:Date,*geom:Polygon:srid=4326"))
        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)
      }

      "insert" >> {
        skipped("integration")

        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)

        val features = createFeatures(sft)

        val fs = ds.getFeatureSource(typeName)
        val ids = fs.addFeatures(new ListFeatureCollection(sft, features))
        ids.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))
      }

      "query" >> {
        skipped("integration")

        val sft = ds.getSchema(typeName)
        println(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
        sft must not(beNull)

        val features = createFeatures(sft)

        testQuery(ds, typeName, "INCLUDE", null, features)
        testQuery(ds, typeName, "IN('0', '2')", null, Seq(features(0), features(2)))
        testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, features.dropRight(2))
        testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, features.dropRight(4))
      }
    }
  }

  def testQuery(ds: DataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
    val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    features must containTheSameElementsAs(results)
  }

}
