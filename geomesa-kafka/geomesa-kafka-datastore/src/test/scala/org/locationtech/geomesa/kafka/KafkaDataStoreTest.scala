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
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryCollection, LineString}
import kafka.server.KafkaConfig
import kafka.utils.{TestUtils, TestZKUtils, Utils}
import org.apache.commons.lang3.RandomUtils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.geotools.data._
import org.geotools.data.store.ContentFeatureStore
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder, ReferencedEnvelope}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Random

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

object TestHarness extends App {

  val gf = JTSFactoryFinder.getGeometryFactory
  val geoCalc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  val ds = DataStoreFinder.getDataStore(
    Map(
      "brokers" -> "dresman:9092",
      "zookeepers" -> "dzoo1",
      "isProducer" -> "true"
    ))

  val sft = SimpleFeatureTypes.createType("thresherlive", "thresherId:String,hdg:Double,projected:List[Boolean],dtg:Date,*geom:MultiLineString:srid=4326")

  if(!ds.getTypeNames.contains("thresherlive")) {
    ds.createSchema(sft)
  }

  val fs = ds.getFeatureSource("thresherlive").asInstanceOf[ContentFeatureStore]

  def randomPt = new Coordinate(-150.0 + 300*Random.nextDouble(), -70.0 + 140.0*Random.nextDouble())

  def buildTrack(id: String): SimpleFeature = {
    val sf = new AvroSimpleFeature(new FeatureIdImpl(id), sft)

    val start = randomPt
    val hdg = -180.0 + 360.0*Random.nextDouble()
    sf.setAttribute("hdg", java.lang.Double.valueOf(hdg))
    geoCalc.setStartingPosition(JTS.toDirectPosition(start, DefaultGeographicCRS.WGS84))
    geoCalc.setDirection(hdg, Random.nextDouble()*30)
    val end = JTS.toGeometry(geoCalc.getDestinationPosition)
    val geom = gf.createMultiLineString(Array(gf.createLineString(Array(start, end.getCoordinate))))
    sf.setDefaultGeometry(geom)
    sf.setAttribute("dtg", new java.util.Date())
    sf.setAttribute("projected", Seq(false))
    sf
  }

  import org.locationtech.geomesa.utils.geotools.Conversions._
  def updateTrack(sf: SimpleFeature, maxDist: Int = 10000): SimpleFeature = {
    val hdg = sf.getAttribute("hdg").asInstanceOf[Double]
    val mls = sf.multiLineString
    val ls = mls.getGeometryN(0).asInstanceOf[LineString]
    val start = ls.getCoordinateN(1)
    geoCalc.setStartingPosition(JTS.toDirectPosition(start, DefaultGeographicCRS.WGS84))
    val candidateNewHdg = hdg * RandomUtils.nextDouble(0.8, 1.2)
    val sign = if(candidateNewHdg < 0) -1.0 else 1.0
    val newHdg = sign * (math.abs(candidateNewHdg)%180.0)
    sf.setAttribute("hdg", newHdg)
    geoCalc.setDirection(newHdg, Random.nextDouble()*maxDist)
    val end = JTS.toGeometry(geoCalc.getDestinationPosition).getCoordinate
    val newGeom = gf.createMultiLineString(Array(gf.createLineString(Array(start, end))) ++ mls.geometries[LineString].take(100))
    sf.setDefaultGeometry(newGeom)
    sf.setAttribute("projected", true :: Array.fill(newGeom.getNumGeometries-1)(false).toList)
    sf
  }

  implicit class RichGeometryCollection(val g: GeometryCollection) extends AnyVal {
    def geometries[T <: Geometry](implicit ct: ClassTag[T]): Array[T] = (0 until g.getNumGeometries).map { i => g.getGeometryN(i).asInstanceOf[T] }.toArray
  }

  val ff = CommonFactoryFinder.getFilterFactory2
  val validBbox = new ReferencedEnvelope(-160, 160, -70, 70, DefaultGeographicCRS.WGS84)
  def updateTrackAndPersist(trk: SimpleFeature): SimpleFeature = {
    val updated = updateTrack(trk, 100000)
    if(validBbox.contains(updated.multiLineString.getGeometryN(0).asInstanceOf[LineString].getCoordinateN(1))) {
      fs.addFeatures(DataUtilities.collection(updated))
      updated
    } else {
      fs.removeFeatures(ff.id(updated.getIdentifier))
      val f = buildTrack(UUID.randomUUID().toString)
      fs.addFeatures(DataUtilities.collection(f))
      f
    }
  }
}
