/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong
import java.{util => ju}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.FeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.GeoMessage
import org.locationtech.geomesa.kafka10.KafkaDataStore.FeatureSourceFactory
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
                                producer: KafkaProducer[Array[Byte], Array[Byte]],
                                q: Query)
  extends ContentFeatureStore(entry, q) with Closeable with LazyLogging {

  private val writerPool = ObjectPoolFactory(getWriterInternal(null, 0).asInstanceOf[KafkaFeatureWriter], 5)

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)

  override def buildFeatureType(): SimpleFeatureType = sft

  override def addFeatures(featureCollection: FeatureCollection[SimpleFeatureType, SimpleFeature]): ju.List[FeatureId] = {
    writerPool.withResource { fw =>
      val ret = Array.ofDim[FeatureId](featureCollection.size())
      val iter = featureCollection.features()
      var i = 0
      while (iter.hasNext) {
        val sf = iter.next()
        ret(i) = sf.getIdentifier
        fw.write(sf)
        i += 1
      }
      ret.toList
    }
  }

  override def removeFeatures(filter: Filter): Unit = filter match {
    case Filter.INCLUDE => clearFeatures()
    case _ => super.removeFeatures(filter)
  }

  def clearFeatures(): Unit = writerPool.withResource { fw => fw.send(GeoMessage.clear()) }

  override def getWriterInternal(query: Query, flags: Int) = {
    if (query == null || query == Query.ALL || query.getFilter == null || query.getFilter == Filter.INCLUDE) {
      new KafkaFeatureWriterAppend(sft, producer, topic)
    } else {
      new KafkaFeatureWriterModify(sft, producer, topic, query)
    }
  }

  override def getCountInternal(query: Query): Int = 0

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null

  override def close(): Unit = producer.close()
}

abstract class KafkaFeatureWriter(sft: SimpleFeatureType, producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String)
  extends FeatureWriter[SimpleFeatureType, SimpleFeature] with LazyLogging {

  protected val msgEncoder = new KafkaGeoMessageEncoder(sft)

  private[kafka10] def write(sf: SimpleFeature): Unit = send(GeoMessage.createOrUpdate(sf))

  private[kafka10] def send(msg: GeoMessage): Unit = {
    logger.debug("sending message: {}", msg)
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, msgEncoder.encodeMessage(topic, msg).key, msgEncoder.encodeMessage(topic, msg).message))
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}

class KafkaFeatureWriterAppend(sft: SimpleFeatureType, producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String)
  extends KafkaFeatureWriter(sft, producer, topic) {

  protected val reuse = new ScalaSimpleFeature("", sft)

  protected def nextId: String = KafkaProducerFeatureStoreFactory.FeatureIds.getAndIncrement().toString

  // always return false as we're appending, per the geotools API
  override def hasNext: Boolean = false

  override def next(): SimpleFeature = {
    reuse.getIdentifier.setID(nextId)
    reuse.getUserData.clear()
    var i = 0
    while (i < reuse.values.length) {
      reuse.values(i) = null
      i += 1
    }
    reuse
  }

  override def write(): Unit = write(reuse)

  override def remove(): Unit = throw new NotImplementedError("Remove called on FeatureWriterAppend")
}

class KafkaFeatureWriterModify(sft: SimpleFeatureType, producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String, query: Query)
  extends KafkaFeatureWriterAppend(sft, producer, topic) {

  private val ids = query.getFilter match {
    case ids: Id => ids.getIDs.iterator
    case _ => throw new NotImplementedError("Only modify by ID is supported")
  }

  override protected def nextId: String = ids.next.toString

  override def hasNext: Boolean = ids.hasNext

  override def remove(): Unit = send(GeoMessage.delete(reuse.getID))
}

object KafkaProducerFeatureStoreFactory {

  private[kafka10] val FeatureIds = new AtomicLong(0L)

  def apply(broker: String): FeatureSourceFactory = {

    val props = new ju.Properties()
    props.put(KafkaUtils10.brokerParam, broker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    (entry: ContentEntry, query: Query, schemaManager: KafkaDataStoreSchemaManager) => {
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)
      val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
      new KafkaProducerFeatureStore(entry, fc.sft, fc.topic, broker, kafkaProducer, query)
    }
  }
}
