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
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends Specification with Logging {

  sequential

  val brokerConf = TestUtils.createBrokerConfig(1)
  brokerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
  brokerConf.put("request.required.acks", "1")
  brokerConf.put("producer.type", "sync")

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

      "and be read from other data stores" >> {
        val readSF = consumerFC.getFeatures.features().next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
      }

      "allow features to be deleted" >> {
        store.removeFeatures(ff.id(ff.featureId("1")))
        Thread.sleep(500) // ensure FC has seen the delete
        consumerFC.getCount(Query.ALL) must be equalTo 0
      }

      "allow modifications of existing features" >> {
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
