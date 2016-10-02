/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicInteger

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreMaxConcurrentTest extends Specification {

  sequential

  protected val sftBaseName = getClass.getSimpleName
  private val sfts = ArrayBuffer.empty[SimpleFeatureType]

  val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  val dsParams = Map(
    "connector" -> connector,
    "caching"   -> false,
    AccumuloDataStoreParams.numConcurrentQueriesParam.getName -> "2",
    // note the table needs to be different to prevent testing errors
    "tableName" -> sftBaseName)

  val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  def createNewSchema(spec: String,
                      dtgField: Option[String] = Some("dtg"),
                      tableSharing: Boolean = true): SimpleFeatureType = synchronized {
    val sftName = sftBaseName
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    ds.createSchema(sft)
    val reloaded = ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
    sfts += reloaded
    reloaded
  }

  def addFeature(sft: SimpleFeatureType, feature: SimpleFeature): java.util.List[FeatureId] =
    addFeatures(sft, Seq(feature))

  /**
    * Call to load the test features into the data store
    */
  def addFeatures(sft: SimpleFeatureType, features: Seq[SimpleFeature]): java.util.List[FeatureId] = {
    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
    features.foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    ds.getFeatureSource(sft.getTypeName).addFeatures(featureCollection)
  }


  "Concurrent queries" should {
    val ff = CommonFactoryFinder.getFilterFactory2
    val defaultSft = createNewSchema("name:String,*geom:Point:srid=4326,dtg:Date:default=true")
    addFeature(defaultSft, ScalaSimpleFeature.create(defaultSft, "fid-1", "name1", "POINT(45 49)", "2010-05-07T12:30:00.000Z"))

    "be limited by configuration" >> {
      val fs = ds.getFeatureSource(defaultSft.getTypeName)
      val atomicInt = new AtomicInteger(0)

      // vary ranges
      val f1 = CQL.toFilter(s"BBOX(geom, 40,40,50,50)")
      val q1 = new Query(defaultSft.getTypeName, f1)

      val f2 = CQL.toFilter(s"BBOX(geom, 30,40,50,50)")
      val q2 = new Query(defaultSft.getTypeName, f2)

      val queries = Array(q1, q2)
      val futures = (0 until 100).map { i => queryAndIterate(atomicInt, fs, queries(Random.nextInt(1))) }

      var maxConcurrent = 0
      while(!futures.exists(_.isCompleted)) {
        val sample = atomicInt.get()
        if(sample > maxConcurrent) {
          maxConcurrent = sample
        }
      }

      "max concurrent must be less than 2" >> { maxConcurrent must beLessThanOrEqualTo(2) }
      "all futures must be 1" >> { futures.map(_.value.get.get).sum must be equalTo 100  }
    }
  }

  def queryAndIterate(atomicCount: AtomicInteger, fs: SimpleFeatureSource, query: Query): Future[Int] = {
    Future {
      val results = fs.getFeatures(query)
      var resultCount = 0
      // have to work around self closing iterators
      val delegate = results.features()
      atomicCount.incrementAndGet()
      val iter = new Iterator[SimpleFeature] {
        var end = false
        override def hasNext: Boolean = {
          if(end) {
            atomicCount.decrementAndGet()
          }
          delegate.hasNext
        }

        override def next(): SimpleFeature = {
          // there's only 1 value
          end = true
          delegate.next()
        }
      }
      iter.foreach { _ => resultCount += 1 }
      resultCount
    }
  }
}
