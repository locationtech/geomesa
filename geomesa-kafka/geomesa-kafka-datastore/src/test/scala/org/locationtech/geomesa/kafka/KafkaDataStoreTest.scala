/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data._
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with TestKafkaServer with Logging {

  sequential

  val ff = CommonFactoryFinder.getFilterFactory2
  val consumerParams = Map(
    "brokers"    -> broker,
    "zookeepers" -> zkConnect,
    "zkPath"     -> "/geomesa/kafka/testds",
    "isProducer" -> false)


  val producerParams = Map(
    "brokers"    -> s"$host:$port",
    "zookeepers" -> zkConnect,
    "zkPath"     -> "/geomesa/kafka/testds",
    "isProducer" -> true)

  val gf = JTSFactoryFinder.getGeometryFactory

  "KafkaDataSource" should {
    import org.locationtech.geomesa.security._

    val consumerDS = DataStoreFinder.getDataStore(consumerParams)
    val producerDS = DataStoreFinder.getDataStore(producerParams)

    "consumerDS must not be null" >> { consumerDS must not beNull }
    "producerDS must not be null" >> { producerDS must not beNull }

    val schema = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

    "allow schemas to be created" >> {
      producerDS.createSchema(schema)

      "and available in other data stores" >> {
        consumerDS.getTypeNames.toList must contain("test")
      }
    }

    "allow features to be written" >> {

      // create the consumerFC first so that it is ready to receive features from the producer
      val consumerFC = consumerDS.getFeatureSource("test")

      val store = producerDS.getFeatureSource("test").asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
      val fw = producerDS.getFeatureWriter("test", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      sf.visibility = "USER|ADMIN"
      fw.write()
      Thread.sleep(2000)

      // AND READ
      {
        val features = consumerFC.getFeatures.features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
        sf.visibility mustEqual Some("USER|ADMIN")
        store.removeFeatures(ff.id(ff.featureId("1")))
        Thread.sleep(500) // ensure FC has seen the delete
        consumerFC.getCount(Query.ALL) must be equalTo 0
      }

      // AND UPDATED
      {
        val updated = sf
        updated.setAttribute("name", "jones")
        updated.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        updated.visibility = "ADMIN"
        store.addFeatures(DataUtilities.collection(updated))

        Thread.sleep(500)
        val q = ff.id(updated.getIdentifier)
        val featureCollection = consumerFC.getFeatures(q)
        featureCollection.size() must be equalTo 1
        val res = featureCollection.features().next()
        res.getAttribute("name") must be equalTo "jones"
        res.visibility mustEqual Some("ADMIN")
      }

      // AND CLEARED
      {
        store.removeFeatures(Filter.INCLUDE)
        Thread.sleep(500)
        consumerFC.getCount(Query.ALL) must be equalTo 0

        val sf = fw.next()
        sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
        sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
        fw.write()

        Thread.sleep(500)
        consumerFC.getCount(Query.ALL) must be equalTo 1
      }

      // ALLOW CQL QUERIES
      {
        val sf = fw.next()
        sf.setAttributes(Array("jones", 60, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
        sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
        sf.visibility = "USER"
        fw.write()

        Thread.sleep(500)
        var res = consumerFC.getFeatures(ff.equals(ff.property("name"), ff.literal("jones")))
        res.size() must be equalTo 1
        val resSF = res.features().next()
        resSF.getAttribute("name") must be equalTo "jones"
        resSF.visibility mustEqual Some("USER")

        res = consumerFC.getFeatures(ff.greater(ff.property("age"), ff.literal(50)))
        res.size() must be equalTo 1
        res.features().next().getAttribute("name") must be equalTo "jones"

        // bbox and cql
        val spatialQ = ff.bbox("geom", -10, -10, 10, 10, "EPSG:4326")
        val attrQ = ff.greater(ff.property("age"), ff.literal(50))
        res = consumerFC.getFeatures(ff.and(spatialQ, attrQ))
        res.size() must be equalTo 1
        res.features().next().getAttribute("name") must be equalTo "jones"
      }
    }

    "return correctly from canProcess" >> {
      import KafkaDataStoreFactoryParams._
      val factory = new KafkaDataStoreFactory
      factory.canProcess(Map.empty[String, Serializable]) must beFalse
      factory.canProcess(Map(KAFKA_BROKER_PARAM.key -> "test", ZOOKEEPERS_PARAM.key -> "test")) must beTrue
    }
  }

  "KafkaDataStore with cachedConsumer" should {
    "expire messages correctly when expirationPeriod is set" >> {
      val cachedConsumerParams = Map(
        "brokers"          -> s"$host:$port",
        "zookeepers"       -> zkConnect,
        "zkPath"           -> "/geomesa/kafka/cachedtestds",
        "isProducer"       -> false,
        "expiry"           -> true,
        "expirationPeriod" -> 2000L)

      val producerParams = Map(
        "brokers"    -> s"$host:$port",
        "zookeepers" -> zkConnect,
        "zkPath"     -> "/geomesa/kafka/cachedtestds",
        "isProducer" -> true)

      //Setup datastores and schema
      val cachedConsumerDS = DataStoreFinder.getDataStore(cachedConsumerParams)
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      val schemaExpiration = SimpleFeatureTypes.createType("testExpiration", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      producerDS.createSchema(schemaExpiration)

      //Setup consumer prior to writing feature so the feature will be written to the cache once the producer writes
      val cachedConsumerFS = cachedConsumerDS.getFeatureSource("testExpiration").asInstanceOf[KafkaConsumerFeatureSource]
      val featureCache = cachedConsumerFS.asInstanceOf[LiveKafkaConsumerFeatureSource].featureCache
      cachedConsumerFS.qt.size() must be equalTo 0

      //Write test feature
      val fw = producerDS.getFeatureWriter("testExpiration", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("jones", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      fw.write()

      featureCache.size() must eventually(10, 500.millis)(beEqualTo(1))
      cachedConsumerFS.qt.size() must eventually(10, 500.millis)(beEqualTo(1))
      Thread.sleep(2000) //sleep enough time to reach the expirationPeriod

      featureCache.cleanUp() //remove old entries now that the TTL has passed
      featureCache.size() must be equalTo 0
      cachedConsumerFS.qt.size() must eventually(10, 500.millis)(beEqualTo(0))
    }
  }

  step {
    shutdown()
  }
}

