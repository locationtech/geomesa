/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import java.util.concurrent.CountDownLatch

import com.google.common.util.concurrent.AtomicLongMap
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, KafkaFeatureEvent, TestLambdaFeatureListener}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class LiveKafkaConsumerFeatureSourceTest extends Specification with HasEmbeddedKafka with LazyLogging {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val gf = JTSFactoryFinder.getGeometryFactory

  "LiveKafkaConsumerFeatureSource" should {
    "allow for configurable expiration" >> {
      val zkPath = "/geomesa/kafka/testexpiry"

      val producerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> true)

      val consumerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> false)

      val sft = {
        val sft = SimpleFeatureTypes.createType("expiry", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)

      val expiryConsumer = DataStoreFinder.getDataStore(consumerParams ++ Map(KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.getName -> 1000L))
      val consumerFC = expiryConsumer.getFeatureSource("expiry")

      val fw = producerDS.getFeatureWriter("expiry", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      fw.write()
      Thread.sleep(500)

      val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

      // verify the feature is written - hit the cache directly
      {
        val features = consumerFC.getFeatures(Filter.INCLUDE).features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
        features.hasNext must beFalse
      }

      // verify the feature is written - hit the spatial index
      {
        val features = consumerFC.getFeatures(bbox).features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
        features.hasNext must beFalse
      }

      // allow the cache to expire
      Thread.sleep(500)

      // verify feature has expired - hit the cache directly
      {
        val features = consumerFC.getFeatures(Filter.INCLUDE).features()
        features.hasNext must beFalse
      }

      // force the cache cleanup - normally this would happen during additional reads and writes
      consumerFC.asInstanceOf[LiveKafkaConsumerFeatureSource].featureCache.cleanUp()

      // verify feature has expired - hit the spatial index
      {
        val features = consumerFC.getFeatures(bbox).features()
        features.hasNext must beFalse
      }
    }

    "support listeners" >> {
      val zkPath = "/geomesa/kafka/testlisteners"

      val producerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> true)

      val consumerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> false)

      val m = AtomicLongMap.create[String]()

      val id = "testlistener"
      val numUpdates = 100
      val maxLon = 80.0
      var latestLon = -1.0

      val sft = {
        val sft = SimpleFeatureTypes.createType("listeners", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)

      val listenerConsumerDS = DataStoreFinder.getDataStore(consumerParams)
      val consumerFC = listenerConsumerDS.getFeatureSource("listeners")

      val latch = new CountDownLatch(numUpdates)

      val featureListener = new TestLambdaFeatureListener((fe: KafkaFeatureEvent) => {
        val f = fe.feature
        val geom: Point = f.getDefaultGeometry.asInstanceOf[Point]
        latestLon = geom.getX
        m.incrementAndGet(f.getID)
        latch.countDown()
      })

      consumerFC.addFeatureListener(featureListener)

      val fw = producerDS.getFeatureWriter("listeners", null, Transaction.AUTO_COMMIT)

      (numUpdates to 1 by -1).foreach { writeUpdate }

      def writeUpdate(i: Int) = {
        val ll = maxLon - maxLon/i

        val sf = fw.next()
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("testlistener")
        sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
        sf.setDefaultGeometry(gf.createPoint(new Coordinate(ll, ll)))
        fw.write()
      }

      logger.debug("Wrote feature")
      while(latch.getCount > 0) {
        Thread.sleep(100)
      }

      logger.debug("getting id")
      m.get(id) must be equalTo numUpdates
      latestLon must be equalTo 0.0
    }

    "handle filters" >> {
      val zkPath = "/geomesa/kafka/testfilters"

      val producerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> true)

      val consumerParams = Map(
        "brokers"    -> brokerConnect,
        "zookeepers" -> zkConnect,
        "zkPath"     -> zkPath,
        "isProducer" -> false)

      val sft = {
        val sft = SimpleFeatureTypes.createType("filts", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)

      val ff = CommonFactoryFinder.getFilterFactory2
      val filt = ff.bbox("geom", -80, 35, -75, 40, "EPSG:4326")
      val listenerConsumerDS = DataStoreFinder.getDataStore(consumerParams).asInstanceOf[KafkaDataStore]
      val consumerFC = listenerConsumerDS.getFeatureSource("filts", filt)

      val fw = producerDS.getFeatureWriter("filts", null, Transaction.AUTO_COMMIT)

      var sf = fw.next()
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("testfilt-1")
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(-77, 38)))
      fw.write()
      logger.debug("Wrote feature")

      sf = fw.next()
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("testfilt-2")
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(-88, 38)))
      fw.write()

      Thread.sleep(1000)

      val features = SelfClosingIterator(consumerFC.getFeatures.features).toList
      features.size must be equalTo 1
      features.head.getID must be equalTo "testfilt-1"
    }
  }

  step {
    shutdown()
  }
}
