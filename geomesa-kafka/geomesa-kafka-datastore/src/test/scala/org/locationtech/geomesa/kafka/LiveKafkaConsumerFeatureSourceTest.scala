/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class LiveKafkaConsumerFeatureSourceTest extends Specification with HasEmbeddedKafka with Logging {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val gf = JTSFactoryFinder.getGeometryFactory

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

  "LiveKafkaConsumerFeatureSource" should {
    "allow for configurable expiration" >> {
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
      consumerFC.asInstanceOf[LiveKafkaConsumerFeatureSource].featureCache.cache.cleanUp()

      // verify feature has expired - hit the spatial index
      {
        val features = consumerFC.getFeatures(bbox).features()
        features.hasNext must beFalse
      }
    }
  }

  step {
    shutdown()
  }
}
