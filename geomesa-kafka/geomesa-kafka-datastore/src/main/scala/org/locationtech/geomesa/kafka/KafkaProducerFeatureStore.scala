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

import java.{util => ju}

import com.vividsolutions.jts.geom.Envelope
import kafka.producer.{ProducerConfig, Producer}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.BridgeIterator
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConversions._

class KafkaProducerFeatureStore(entry: ContentEntry,
                                sft: SimpleFeatureType,
                                topic: String,
                                broker: String,
                                producer: Producer[Array[Byte], Array[Byte]],
                                query: Query = null)
  extends ContentFeatureStore(entry, query) {

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = sft

  type FW = FeatureWriter[SimpleFeatureType, SimpleFeature]

  val writerPool = ObjectPoolFactory(new ModifyingFeatureWriter(query), 5)
  override def addFeatures(featureCollection: FeatureCollection[SimpleFeatureType, SimpleFeature]): ju.List[FeatureId] = {
    writerPool.withResource { fw =>
      val ret = Array.ofDim[FeatureId](featureCollection.size())
      fw.setIter(new BridgeIterator[SimpleFeature](featureCollection.features()))
      var i = 0
      while(fw.hasNext) {
        val sf = fw.next()
        ret(i) = sf.getIdentifier
        i+=1
        fw.write()
      }
      ret.toList
    }
  }

  override def removeFeatures(filter: Filter): Unit = filter match {
    case Filter.INCLUDE => clearFeatures()
    case _              => super.removeFeatures(filter)
  }

  def clearFeatures(): Unit = {
    val msg = GeoMessage.clear()
    producer.send(KafkaGeoMessageEncoder.encodeClearMessage(topic, msg))
  }

  override def getWriterInternal(query: Query, flags: Int) =
    new ModifyingFeatureWriter(query)

  class ModifyingFeatureWriter(query: Query) extends FW {

    val msgEncoder = new KafkaGeoMessageEncoder(sft)

    private var id = 1L
    def getNextId: FeatureId = {
      val ret = id
      id += 1
      new FeatureIdImpl(ret.toString)
    }

    var toModify: Iterator[SimpleFeature] =
      if(query == null) Iterator[SimpleFeature]()
      else if(query.getFilter == null) Iterator.continually(new AvroSimpleFeature(getNextId, sft))
      else query.getFilter match {
        case ids: Id        =>
          ids.getIDs.map(id => new AvroSimpleFeature(new FeatureIdImpl(id.toString), sft)).iterator

        case Filter.INCLUDE =>
          Iterator.continually(new AvroSimpleFeature(new FeatureIdImpl(""), sft))
      }

    def setIter(iter: Iterator[SimpleFeature]): Unit = {
      toModify = iter
    }

    var curFeature: SimpleFeature = null
    override def getFeatureType: SimpleFeatureType = sft

    override def next(): SimpleFeature = {
      curFeature = toModify.next()
      curFeature
    }

    override def remove(): Unit = {
      val msg = GeoMessage.delete(curFeature.getID)
      curFeature = null

      send(msg)
    }

    override def write(): Unit = {
      val msg = GeoMessage.createOrUpdate(curFeature)
      curFeature = null

      send(msg)
    }

    override def hasNext: Boolean = toModify.hasNext
    override def close(): Unit = {}

    private def send(msg: GeoMessage): Unit = {
      producer.send(msgEncoder.encodeMessage(topic, msg))
    }
  }

  override def getCountInternal(query: Query): Int = 0
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null
}

object KafkaProducerFeatureStoreFactory {

  def apply(broker: String): FeatureSourceFactory = {

    val config = {
      val props = new ju.Properties()
      props.put("metadata.broker.list", broker)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")
      new ProducerConfig(props)
    }

    (entry: ContentEntry, fc: KafkaFeatureConfig) => {
      val kafkaProducer = new Producer[Array[Byte], Array[Byte]](config)
      new KafkaProducerFeatureStore(entry, fc.sft, fc.topic, broker, kafkaProducer)
    }
  }
}
