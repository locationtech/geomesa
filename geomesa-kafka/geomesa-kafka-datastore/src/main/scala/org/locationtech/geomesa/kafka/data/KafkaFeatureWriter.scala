/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.Flushable
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.kafka.RecordVersions
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

trait KafkaFeatureWriter extends SimpleFeatureWriter with Flushable {

  /**
    * Sends a 'clear' message that will delete any features written so far
    */
  def clear(): Unit
}

object KafkaFeatureWriter {

  private val featureIds = new AtomicLong(0)

  class AppendKafkaFeatureWriter(sft: SimpleFeatureType,
                                 producer: Producer[Array[Byte], Array[Byte]],
                                 serialization: SerializationType) extends KafkaFeatureWriter with LazyLogging {

    protected val topic: String = KafkaDataStore.topic(sft)

    protected val serializer = GeoMessageSerializer(sft, serialization)

    protected val feature = new ScalaSimpleFeature(sft, "-1")

    override def getFeatureType: SimpleFeatureType = sft

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = {
      reset(featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = {
      val sf = GeoMesaFeatureWriter.featureWithFid(sft, feature)
      logger.debug(s"Writing update to $topic: $sf")
      val (key, value, headers) = serializer.serialize(GeoMessage.change(sf))
      val record = new ProducerRecord(topic, key, value)
      headers.foreach { case (k, v) => RecordVersions.setHeader(record, k, v) }
      producer.send(record)
    }

    override def remove(): Unit = throw new NotImplementedError()

    override def flush(): Unit = producer.flush()

    override def close(): Unit = producer.flush() // note: the producer is shared, so don't close it

    override def clear(): Unit = {
      logger.debug(s"Writing clear to $topic")
      val (key, value, headers) = serializer.serialize(GeoMessage.clear())
      val record = new ProducerRecord(topic, key, value)
      headers.foreach { case (k, v) => RecordVersions.setHeader(record, k, v) }
      producer.send(record)
    }

    protected def reset(id: String): Unit = {
      feature.setId(id)
      var i = 0
      while (i < sft.getAttributeCount) {
        feature.setAttributeNoConvert(i, null)
        i += 1
      }
      feature.getUserData.clear()
      feature
    }
  }

  class ModifyKafkaFeatureWriter(sft: SimpleFeatureType,
                                 producer: Producer[Array[Byte], Array[Byte]],
                                 serialization: SerializationType,
                                 filter: Filter) extends AppendKafkaFeatureWriter(sft, producer, serialization) {

    import scala.collection.JavaConversions._

    private val ids: Iterator[String] = filter match {
      case ids: Id => ids.getIDs.iterator.map(_.toString)
      case _ => throw new NotImplementedError("Only modify by ID is supported")
    }

    override def hasNext: Boolean = ids.hasNext

    override def next(): SimpleFeature = {
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator
      reset(ids.headOption.getOrElse(featureIds.getAndIncrement().toString))
      // default to using the provided fid
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      feature
    }

    override def remove(): Unit = {
      val id = GeoMesaFeatureWriter.featureWithFid(sft, feature).getID
      logger.debug(s"Writing delete to $topic: $id")
      val (key, value, headers) = serializer.serialize(GeoMessage.delete(id))
      val record = new ProducerRecord(topic, key, value)
      headers.foreach { case (k, v) => RecordVersions.setHeader(record, k, v) }
      producer.send(record)
    }
  }
}
