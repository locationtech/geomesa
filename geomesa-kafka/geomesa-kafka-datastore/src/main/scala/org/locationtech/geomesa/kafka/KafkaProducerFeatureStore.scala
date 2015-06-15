/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.kafka

import java.{util => ju}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Envelope
import kafka.producer.{Producer, ProducerConfig}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.BridgeIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.utils.geotools._
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
  extends ContentFeatureStore(entry, query) with Logging {

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)

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
    logger.debug("sending message: {}", msg)

    val encoder = new KafkaGeoMessageEncoder(sft)
    producer.send(encoder.encodeClearMessage(topic, msg))
  }

  override def getWriterInternal(query: Query, flags: Int) =
    new ModifyingFeatureWriter(query)

  class ModifyingFeatureWriter(query: Query) extends FW with Logging {

    val msgEncoder = new KafkaGeoMessageEncoder(sft)
    val reuse = new ScalaSimpleFeature("", sft)

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
          ids.getIDs.map(id => new ScalaSimpleFeature(id.toString, sft)).iterator

        case Filter.INCLUDE =>
          Iterator.continually(new ScalaSimpleFeature("", sft))
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
      logger.debug("sending message: {}", msg)
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

    (entry: ContentEntry, schemaManager: KafkaDataStoreSchemaManager) => {
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)
      val kafkaProducer = new Producer[Array[Byte], Array[Byte]](config)
      new KafkaProducerFeatureStore(entry, fc.sft, fc.topic, broker, kafkaProducer)
    }
  }
}
