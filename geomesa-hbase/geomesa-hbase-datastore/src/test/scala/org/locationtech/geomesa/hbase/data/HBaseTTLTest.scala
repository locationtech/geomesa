/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class HBaseTTLTest extends Specification with LazyLogging {

  sequential

  val typeName = "test_data"
  val params = Map(
    ConnectionParam.getName -> MiniCluster.connection,
    HBaseCatalogParam.getName -> getClass.getSimpleName)

  val durationStr = "4 seconds"
  val expiration = f"dtg($durationStr)"
  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(typeName, f"name:String:index=true,dtg:Date;geomesa.feature.expiry=$expiration")
  // NOTE: expiration date is set simply by adding expiration time to when the feature is created
  val ttl = Duration(durationStr).toMillis

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  "HBase TTL" should {
    "remove X features based on TTL" >> {
      val numFeatures = 3
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)
      ds.createSchema(sft)

      try {
        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val startTime = new Date(System.currentTimeMillis())

        val toAdd = (0 until numFeatures).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")

          val featureTime = new Date(startTime.getTime + (ttl / 2) * i)
          sf.setAttribute(1, dateFormat.format(featureTime))
          sf
        }

        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until numFeatures).map(_.toString))

        getElements(ds, typeName).size mustEqual numFeatures

        Thread.sleep(ttl) // wait until one feature times out
        (1 until numFeatures).map { i =>
          getElements(ds, typeName).size mustEqual numFeatures - i
          Thread.sleep(ttl / 2) // wait for next feature to time out
        }
        true
      } finally {
        ds.dispose()
      }
    }

    "refuse to add already-expired features" >> {
      val numFeatures = 2; // one expired, other not
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)
      ds.createSchema(sft)

      try {
        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val toAdd = (0 until numFeatures).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")

          val featureTime = new Date(System.currentTimeMillis() - ttl * i)
          //          logger.info(f"to be deleted at ${featureTime.getTime}, 10 secs from now is ${System.currentTimeMillis() + 10000}")
          sf.setAttribute(1, dateFormat.format(featureTime))
          sf
        }
        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until numFeatures).map(_.toString))

        // one element should've been rejected
        getElements(ds, typeName).size mustEqual 1
      } finally {
        ds.dispose()
      }
    }
  }

  def getElements(ds: HBaseDataStore,
                  typeName: String): List[SimpleFeature] = {
    runQuery(ds, new Query(typeName))
  }

  def runQuery(ds: HBaseDataStore,
               typeName: String,
               filter: String): List[SimpleFeature] = {
    runQuery(ds, new Query(typeName, ECQL.toFilter(filter)))
  }

  def runQuery(ds: HBaseDataStore,
               query: Query): List[SimpleFeature] = {
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    features
  }
}
