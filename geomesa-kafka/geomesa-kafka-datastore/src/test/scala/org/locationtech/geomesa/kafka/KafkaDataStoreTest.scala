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

import java.net.InetSocketAddress

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, TestZKUtils, Utils}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
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
class KafkaDataStoreTest extends Specification with Logging {

  sequential

  val brokerConf = TestUtils.createBrokerConfig(1)

  val zkConnect = TestZKUtils.zookeeperConnect
  val zk = new EmbeddedZookeeper(zkConnect)
  val server = TestUtils.createServer(new KafkaConfig(brokerConf))

  val host = brokerConf.getProperty("host.name")
  val port = brokerConf.getProperty("port").toInt
  val ff = CommonFactoryFinder.getFilterFactory2

  "KafkaDataSource" should {
    val consumerParams = Map(
      "brokers"     -> s"$host:$port",
      "zookeepers" -> zkConnect,
      "isProducer" -> false)

    val consumerDS = DataStoreFinder.getDataStore(consumerParams)

    val producerParams = Map(
      "brokers"     -> s"$host:$port",
      "zookeepers" -> zkConnect,
      "isProducer" -> true)

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

    val gf = JTSFactoryFinder.getGeometryFactory
    "allow features to be written" >> {
      // create the consumerFC first so that it is ready to receive features from the producer
      val consumerFC = consumerDS.getFeatureSource("test")

      val store = producerDS.getFeatureSource("test").asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]
      val fw = producerDS.getFeatureWriter("test", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      fw.write()
      Thread.sleep(2000)

      // AND READ
      {
        val features = consumerFC.getFeatures.features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")

        store.removeFeatures(ff.id(ff.featureId("1")))
        Thread.sleep(500) // ensure FC has seen the delete
        consumerFC.getCount(Query.ALL) must be equalTo 0
      }

      // AND UPDATED
      {
        val updated = sf
        updated.setAttribute("name", "jones")
        updated.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        store.addFeatures(DataUtilities.collection(updated))

        Thread.sleep(500)
        val q = ff.id(updated.getIdentifier)
        val featureCollection = consumerFC.getFeatures(q)
        featureCollection.size() must be equalTo 1
        val res = featureCollection.features().next()
        res.getAttribute("name") must be equalTo "jones"
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
        fw.write()

        Thread.sleep(500)
        var res = consumerFC.getFeatures(ff.equals(ff.property("name"), ff.literal("jones")))
        res.size() must be equalTo 1
        res.features().next().getAttribute("name") must be equalTo "jones"

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
  }

  step {
    try {
      server.shutdown()
      zk.shutdown()
    } catch {
      case _: Throwable =>
    }
  }
}


class EmbeddedZookeeper(val connectString: String) {
  val snapshotDir = TestUtils.tempDir()
  val logDir = TestUtils.tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val port = connectString.split(":")(1).toInt
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress("127.0.0.1", port), 1024)
  factory.startup(zookeeper)

  def shutdown() {
    try { zookeeper.shutdown() } catch { case _: Throwable => }
    try { factory.shutdown() } catch { case _: Throwable => }
    Utils.rm(logDir)
    Utils.rm(snapshotDir)
  }

}
