/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.Params._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HBaseDataStoreTest extends Specification with LazyLogging {

  sequential

  val cluster = new HBaseTestingUtility()
  var connection: Connection = null

  step {
    logger.info("Starting embedded hbase")
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started")
  }

  "HBaseDataStore" should {
    "work with points" in {
      val typeName = "testpoints"

      val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> "test_sft")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      testQuery(ds, typeName, "INCLUDE", null, toAdd)
      testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
      testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", null, toAdd.dropRight(2))
      testQuery(ds, typeName, "bbox(geom,42,48,52,62)", null, toAdd.drop(2))
      testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
    }

    "work with polys" in {
      val typeName = "testpolys"

      val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> "test_sft")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Polygon:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
        sf.setAttribute(2, s"POLYGON((-120 4$i, -120 50, -125 50, -125 4$i, -120 4$i))")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      testQuery(ds, typeName, "INCLUDE", null, toAdd)
      testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
      testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
      testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
      testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
    }
  }

  def testQuery(ds: DataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
    val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    features must containTheSameElementsAs(results)
  }

  step {
    logger.info("Stopping embedded hbase")
    cluster.shutdownMiniCluster()
    logger.info("Stopped")
  }
}
