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

import java.nio.charset.StandardCharsets
import java.{util => ju}

import com.vividsolutions.jts.geom.Envelope
import kafka.producer.{KeyedMessage, Producer}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.BridgeIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.{Filter, Id}

import scala.collection.JavaConversions._

object KafkaProducerFeatureStore {
  val DELETE_KEY = "delete".getBytes(StandardCharsets.UTF_8)
  val CLEAR_KEY  = "clear".getBytes(StandardCharsets.UTF_8)
}

class KafkaProducerFeatureStore(entry: ContentEntry,
                                schema: SimpleFeatureType,
                                broker: String,
                                query: Query,
                                producer: Producer[Array[Byte], Array[Byte]])
  extends ContentFeatureStore(entry, query) {

  val typeName = entry.getTypeName

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

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

  def clearFeatures(): Unit = producer.send(Clear.toMsg(typeName))

  type MSG = KeyedMessage[Array[Byte], Array[Byte]]

  override def getWriterInternal(query: Query, flags: Int) =
    new ModifyingFeatureWriter(query)

  class ModifyingFeatureWriter(query: Query) extends FW {

    val encoder = new KryoFeatureSerializer(schema, SerializationOptions.withUserData)
    val reuse = new ScalaSimpleFeature("", schema)
    private var id = 1L
    def getNextId: String = {
      val ret = id
      id += 1
      s"$ret"
    }

    var toModify: Iterator[SimpleFeature] =
      if(query == null) Iterator[SimpleFeature]()
      else if(query.getFilter == null) Iterator.continually {
        reuse.getIdentifier.setID(getNextId)
        reuse
      }
      else query.getFilter match {
        case ids: Id        =>
          ids.getIDs.map(id => new ScalaSimpleFeature(id.toString, schema)).iterator

        case Filter.INCLUDE =>
          Iterator.continually(new ScalaSimpleFeature("", schema))
      }

    def setIter(iter: Iterator[SimpleFeature]): Unit = {
      toModify = iter
    }

    var curFeature: SimpleFeature = null
    override def getFeatureType: SimpleFeatureType = schema
    override def next(): SimpleFeature = {
      curFeature = toModify.next()
      curFeature
    }
    override def remove(): Unit = {
      val bytes = curFeature.getID.getBytes(StandardCharsets.UTF_8)
      val delMsg = new MSG(typeName, KafkaProducerFeatureStore.DELETE_KEY, bytes)
      curFeature = null
      producer.send(delMsg)
    }
    override def write(): Unit = {
      val encoded = encoder.serialize(curFeature)
      val msg = new MSG(typeName, encoded)
      curFeature = null
      producer.send(msg)
    }
    override def hasNext: Boolean = toModify.hasNext
    override def close(): Unit = {}
  }

  override def getCountInternal(query: Query): Int = 0
  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null
}
