/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka21

import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.{GeoMessage, GeoMessageEncoder}
import org.locationtech.geomesa.kafka21.KafkaProducerFeatureStore.{KafkaFeatureWriter, KafkaFeatureWriterAppend, KafkaFeatureWriterModify}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

class KafkaProducerFeatureStore(
    entry: ContentEntry,
    sft: SimpleFeatureType,
    topic: String,
    producer: Producer[Array[Byte], Array[Byte]]
  ) extends ContentFeatureStore(entry, null) with Closeable with LazyLogging {

  private val encoder = new GeoMessageEncoder(sft)

  override def getBoundsInternal(query: Query): ReferencedEnvelope =
    org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

  override def buildFeatureType(): SimpleFeatureType = sft

  override def removeFeatures(filter: Filter): Unit = filter match {
    case Filter.INCLUDE =>
      val msg = GeoMessage.clear()
      producer.send(new ProducerRecord(topic, encoder.encodeKey(msg), encoder.encodeMessage(msg)))
    case _ => super.removeFeatures(filter)
  }

  override def getWriterInternal(query: Query, flags: Int): KafkaFeatureWriter = {
    if (query != null && query != Query.ALL && query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      new KafkaFeatureWriterModify(sft, producer, topic, query, encoder)
    } else {
      new KafkaFeatureWriterAppend(sft, producer, topic, encoder)
    }
  }

  override def getCountInternal(query: Query): Int = 0

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = null

  override def close(): Unit = producer.close()

  override protected def canFilter: Boolean = true

  override protected def buildQueryCapabilities(): QueryCapabilities = {
    new QueryCapabilities() {
      override def isUseProvidedFIDSupported: Boolean = true
    }
  }
}

object KafkaProducerFeatureStore {

  private val FeatureIds = new AtomicLong(0L)

  abstract class KafkaFeatureWriter(
      sft: SimpleFeatureType,
      producer: Producer[Array[Byte], Array[Byte]],
      topic: String,
      encoder: GeoMessageEncoder
    ) extends FeatureWriter[SimpleFeatureType, SimpleFeature] with LazyLogging {

    protected def send(msg: GeoMessage): Unit = {
      logger.debug(s"Sending message on topic $topic: $msg")
      producer.send(new ProducerRecord(topic, encoder.encodeKey(msg), encoder.encodeMessage(msg)))
    }

    override def getFeatureType: SimpleFeatureType = sft

    override def close(): Unit = producer.flush()
  }

  class KafkaFeatureWriterAppend(
      sft: SimpleFeatureType,
      producer: Producer[Array[Byte], Array[Byte]],
      topic: String,
      encoder: GeoMessageEncoder
    ) extends KafkaFeatureWriter(sft, producer, topic, encoder) {

    protected val reuse = new ScalaSimpleFeature(sft, "")

    protected def nextId: String = FeatureIds.getAndIncrement().toString

    // always return false as we're appending, per the geotools API
    override def hasNext: Boolean = false

    override def next(): SimpleFeature = {
      reuse.setId(nextId)
      reuse.getUserData.clear()
      var i = 0
      while (i < reuse.getAttributeCount) {
        reuse.setAttribute(i, null)
        i += 1
      }
      reuse
    }

    override def write(): Unit = {
      // content feature store puts the original feature in the user data... remove it so it's not serialized twice
      reuse.getUserData.remove(ContentFeatureStore.ORIGINAL_FEATURE_KEY)
      send(GeoMessage.createOrUpdate(reuse))
    }

    override def remove(): Unit = throw new NotImplementedError("Remove called on FeatureWriterAppend")
  }

  class KafkaFeatureWriterModify(
      sft: SimpleFeatureType,
      producer: Producer[Array[Byte], Array[Byte]],
      topic: String,
      query: Query,
      encoder: GeoMessageEncoder
    ) extends KafkaFeatureWriterAppend(sft, producer, topic, encoder) {

    private val ids = query.getFilter match {
      case ids: Id => ids.getIDs.iterator
      case _ => throw new NotImplementedError("Only modify by ID is supported")
    }

    override protected def nextId: String = ids.next.toString

    override def hasNext: Boolean = ids.hasNext

    override def remove(): Unit = send(GeoMessage.delete(reuse.getID))
  }
}
