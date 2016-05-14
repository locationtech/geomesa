/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.ReplayTimeHelper.ff
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with HasEmbeddedKafka with LazyLogging {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val gf = JTSFactoryFinder.getGeometryFactory

  val zkPath = "/geomesa/kafka/testds"

  val producerParams = Map(
    "brokers"    -> brokerConnect,
    "zookeepers" -> zkConnect,
    "zkPath"     -> zkPath,
    "isProducer" -> true)

  "KafkaDataSource" should {
    import org.locationtech.geomesa.security._

    val consumerParams = Map(
      "brokers"    -> brokerConnect,
      "zookeepers" -> zkConnect,
      "zkPath"     -> zkPath,
      "isProducer" -> false)

    val consumerDS = DataStoreFinder.getDataStore(consumerParams)
    val producerDS = DataStoreFinder.getDataStore(producerParams)

    "consumerDS must not be null" >> { consumerDS must not beNull }
    "producerDS must not be null" >> { producerDS must not beNull }

    val schema = {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
    }

    "allow schemas to be created" >> {
      producerDS.createSchema(schema)
      "and available in other data stores" >> {
        consumerDS.getTypeNames.toList must contain("test")
      }
      ok
    }

    "allow schemas to be created with user-data" >> {
      val schemaWithMetadata = {
        val sft = SimpleFeatureTypes.createType("user-data",
          "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      producerDS.createSchema(schemaWithMetadata)
      "and available in other data stores" >> {
        val retrieved = consumerDS.getSchema("user-data")
        retrieved must not(beNull)
        retrieved.getUserData.get("geomesa.foo") mustEqual "bar"
      }
      ok
    }

    "allow schemas to be deleted" >> {
      val replaySFT = KafkaDataStoreHelper.createReplaySFT(schema, ReplayConfig(10000L, 20000L, 1000L))
      val name = replaySFT.getTypeName

      consumerDS.createSchema(replaySFT)
      consumerDS.getTypeNames.toList must contain(name)

      consumerDS.removeSchema(name)
      consumerDS.getTypeNames.toList must not(contain(name))
    }

    "allow features to be written" >> {

      // create the consumerFC first so that it is ready to receive features from the producer
      val consumerFC = consumerDS.getFeatureSource("test")

      val store = producerDS.getFeatureSource("test").asInstanceOf[SimpleFeatureStore]
      val fw = producerDS.getFeatureWriter("test", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      sf.visibility = "USER|ADMIN"
      fw.write()
      Thread.sleep(2000)

      "and read" >> {
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

      "and updated" >> {
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

      "and cleared" >> {
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

      "and queried with cql" >> {
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
      ok
    }

    "return correctly from canProcess" >> {
      import KafkaDataStoreFactoryParams._
      val factory = new KafkaDataStoreFactory
      factory.canProcess(Map.empty[String, Serializable]) must beFalse
      factory.canProcess(Map(KAFKA_BROKER_PARAM.key -> "test", ZOOKEEPERS_PARAM.key -> "test")) must beTrue
    }
  }

  step {
    shutdown()
  }
}
