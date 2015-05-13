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

import java.io.Serializable
import java.{util => ju}

import kafka.producer.{Producer, ProducerConfig}
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.{DataStore, Query}
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.{Duration, Instant}
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ReplayKafkaDataStoreTest extends Specification with HasEmbeddedZookeeper {

  sequential

  val typeName = "track"
  val schema = SimpleFeatureTypes.createType(typeName, "trackId:String,*geom:LineString:srid=4326")

  // the topic name is not configurable - it is the same as the name of the SFT
  val topic = typeName

  val track0v0 = track("track0", "Point      (30 30)")
  val track0v1 = track("track0", "LineString (30 30, 35 30)")
  val track0v2 = track("track0", "LineString (30 30, 35 30, 40 34)")
  val track0v3 = track("track0", "LineString (30 30, 35 32, 40 34, 45 36)")

  val track1v0 = track("track1", "Point      (50 20)")
  val track1v1 = track("track1", "LineString (50 20, 40 30)")
  val track1v2 = track("track1", "LineString (50 20, 40 30, 30 30)")

  val track2v0 = track("track2", "Point      (30 30)")
  val track2v1 = track("track2", "LineString (30 30, 30 25)")
  val track2v2 = track("track2", "LineString (30 30, 30 25, 28 20)")
  val track2v3 = track("track2", "LineString (30 30, 30 25, 25 20, 20 15)")

  val track3v0 = track("track3", "Point      (0 60)")
  val track3v1 = track("track3", "LineString (0 60, 10 60)")
  val track3v2 = track("track3", "LineString (0 60, 10 60, 20 55)")
  val track3v3 = track("track3", "LineString (0 60, 10 60, 20 55, 30 40)")
  val track3v4 = track("track3", "LineString (0 60, 10 60, 20 55, 30 40, 30 30)")

  val messages: Seq[GeoMessage] = Seq(

                                                    // offset
    CreateOrUpdate(new Instant(10993), track0v0),   //  0
    CreateOrUpdate(new Instant(11001), track3v0),   //  1

    CreateOrUpdate(new Instant(11549), track3v1),   //  2

    CreateOrUpdate(new Instant(11994), track0v1),   //  3
    CreateOrUpdate(new Instant(11995), track1v0),   //  4
    CreateOrUpdate(new Instant(11995), track3v2),   //  5

    CreateOrUpdate(new Instant(12998), track1v1),   //  6
    CreateOrUpdate(new Instant(13000), track2v0),   //  7
    CreateOrUpdate(new Instant(13002), track3v3),   //  8
    CreateOrUpdate(new Instant(13002), track0v2),   //  9

    CreateOrUpdate(new Instant(13444), track1v2),   // 10

    CreateOrUpdate(new Instant(13096), track2v1),   // 11
    CreateOrUpdate(new Instant(13099), track3v4),   // 12
    CreateOrUpdate(new Instant(14002), track0v3),   // 13
    Delete(        new Instant(14005), "track1"),   // 14

    Delete(        new Instant(14990), "track3"),   // 15
    CreateOrUpdate(new Instant(14999), track2v2),   // 16
    Delete(        new Instant(15000), "track0"),   // 17

    Clear(         new Instant(16003)),             // 18

    CreateOrUpdate(new Instant(16997), track2v3),   // 19

    Delete(        new Instant(17000), "track3")    // 20

  )

  createTopic()
  sendMessages()

  "replay" should {

    "select the most recent version within the replay window when no message time is given" >> {

      val ds = createDataStore(10000, 12000, 1000)
      val fs = ds.getFeatureSource(typeName)
      fs.isInstanceOf[ReplayKafkaConsumerFeatureSource] must beTrue

      val features = featuresToList(fs.getFeatures)

      features must haveSize(3)
      features must contain(track0v1, track1v0, track3v2)
    }

    "use the message time when given" >> {

      val ds = createDataStore(10000, 20000, 1000)
      val fs = ds.getFeatureSource(typeName)

      "in a filter" >> {
        val filter = ReplayKafkaConsumerFeatureSource.messageTimeEquals(new Instant(13000))

        val features = featuresToList(fs.getFeatures(filter))

        features must haveSize(2)
        features must contain(track1v1, track2v0)
      }

      "in a query" >> {
        val filter = ReplayKafkaConsumerFeatureSource.messageTimeEquals(new Instant(13000))
        val query = new Query()
        query.setFilter(filter)

        val features = featuresToList(fs.getFeatures(filter))

        features must haveSize(2)
        features must contain(track1v1, track2v0)
      }
    }

    "find messages with the same time" >> {

      val ds = createDataStore(10000, 15000, 1000)
      val fs = ds.getFeatureSource(typeName)

      val filter = ReplayKafkaConsumerFeatureSource.messageTimeEquals(new Instant(12000))

      val features = featuresToList(fs.getFeatures(filter))

      features must haveSize(3)
      features must contain(track0v1, track1v0, track3v2)
    }
  }

  step {
    shutdown()
  }

  def featuresToList(sfc: SimpleFeatureCollection): List[SimpleFeature] = {
    val iter: RichSimpleFeatureIterator = sfc.features()
    val features = iter.toList
    iter.close()
    features
  }

  def track(id: String, track: String): SimpleFeature = {
    val geom = WKTUtils.read(track)
    new SimpleFeatureImpl(List[Object](id, geom), schema, new FeatureIdImpl(id))
  }

  def createTopic(): Unit = {
    // the topic must be created by KafkaDataStore because it also stores some data in zookeeper

    val props = Map(
      KAFKA_BROKER_PARAM.key -> brokerConnect,
      ZOOKEEPERS_PARAM.key -> zkConnect,
      IS_PRODUCER_PARAM.key -> true.asInstanceOf[Serializable]
    )

    new KafkaDataStoreFactory().createDataStore(props).createSchema(schema)
  }

  def sendMessages(): Unit = {
    val props = new ju.Properties()
    props.put("metadata.broker.list", brokerConnect)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val kafkaProducer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))

    val encoder = new KafkaGeoMessageEncoder(schema)

    messages.foreach(msg => kafkaProducer.send(encoder.encodeMessage(topic, msg)))
  }

  def createDataStore(start: Long, end: Long, readBehind: Long): DataStore = {
    val props = ReplayKafkaDataStoreFactory.props(
      brokerConnect, zkConnect, new Instant(start), new Instant(end), Duration.millis(readBehind))

    new ReplayKafkaDataStoreFactory().createDataStore(props)
  }

  def contain(sf: SimpleFeature*): Matcher[Seq[SimpleFeature]] = contain(exactly(sf.map(equalSF) : _*))

  def containSF(expected: SimpleFeature): Matcher[Seq[SimpleFeature]] = {
    val matcher = equalSF(expected)

    seq: Seq[SimpleFeature] => seq.exists(matcher.test)
  }

  def equalSF(expected: SimpleFeature): Matcher[SimpleFeature] = {
    sf: SimpleFeature => {
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      sf.getUserData mustEqual expected.getUserData
    }
  }
}
