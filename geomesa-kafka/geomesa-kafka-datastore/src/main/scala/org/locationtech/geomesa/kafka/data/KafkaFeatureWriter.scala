/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.geotools.data.Transaction
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageSerializer}
import org.locationtech.geomesa.kafka.versions.RecordVersions
import org.locationtech.geomesa.security.VisibilityChecker
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

import java.io.Flushable
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

trait KafkaFeatureWriter extends SimpleFeatureWriter with Flushable {

  /**
    * Sends a 'clear' message that will delete any features written so far
    */
  def clear(): Unit
}

object KafkaFeatureWriter {

  private val featureIds = new AtomicLong(0)
  private val FeatureIdHints = Seq(Hints.USE_PROVIDED_FID, Hints.PROVIDED_FID)

  class AppendKafkaFeatureWriter(
      sft: SimpleFeatureType,
      producer: KafkaFeatureProducer,
      protected val serializer: GeoMessageSerializer
    ) extends KafkaFeatureWriter with LazyLogging {

    protected val topic: String = KafkaDataStore.topic(sft)

    protected val feature = new ScalaSimpleFeature(sft, "-1")

    override def getFeatureType: SimpleFeatureType = sft

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = {
      reset(featureIds.getAndIncrement().toString)
      feature
    }

    override def write(): Unit = {
      val sf = GeoMesaFeatureWriter.featureWithFid(feature)
      // we've handled the fid hints, remove them so that we don't serialize them
      FeatureIdHints.foreach(sf.getUserData.remove)
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
    }
  }

  class ModifyKafkaFeatureWriter(
      sft: SimpleFeatureType,
      producer: KafkaFeatureProducer,
      serializer: GeoMessageSerializer,
      filter: Filter
    ) extends AppendKafkaFeatureWriter(sft, producer, serializer) {

    import scala.collection.JavaConverters._

    private val ids: Iterator[String] = filter match {
      case ids: Id => ids.getIDs.iterator.asScala.map(_.toString)
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
      val id = GeoMesaFeatureWriter.featureWithFid(feature).getID
      logger.debug(s"Writing delete to $topic: $id")
      val (key, value, headers) = serializer.serialize(GeoMessage.delete(id))
      val record = new ProducerRecord(topic, key, value)
      headers.foreach { case (k, v) => RecordVersions.setHeader(record, k, v) }
      producer.send(record)
    }
  }

  trait RequiredVisibilityWriter extends AppendKafkaFeatureWriter with VisibilityChecker {
    abstract override def write(): Unit = {
      requireVisibilities(feature)
      super.write()
    }
  }

  trait KafkaFeatureProducer extends Flushable {
    def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit
  }

  case class AutoCommitProducer(producer: Producer[Array[Byte], Array[Byte]]) extends KafkaFeatureProducer {
    override def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = producer.send(record)
    override def flush(): Unit = producer.flush()
  }

  case class KafkaTransactionState(producer: Producer[Array[Byte], Array[Byte]])
      extends Transaction.State with KafkaFeatureProducer {

    private val tx = new AtomicReference[Transaction](null)
    private var inTransaction = false

    override def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (!inTransaction) {
        inTransaction = true
        producer.beginTransaction()
      }
      producer.send(record)
    }

    /**
     * Sends offsets to the consumer coordinator, as part of a consume/transform/produce operation.
     * See `org.apache.kafka.clients.producer.KafkaProducer#sendOffsetsToTransaction(Map,String)`
     *
     * @param offsets offsets to send
     * @param consumerGroupId consumer group id
     */
    def sendOffsets(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): Unit = {
      if (inTransaction) {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId)
      }
    }

    override def flush(): Unit = producer.flush()

    override def setTransaction(transaction: Transaction): Unit = {
      if (transaction == null) {
        // null transaction indicates the transaction has been closed
        try {
          if (inTransaction) {
            inTransaction = false
            producer.commitTransaction()
          }
        } finally {
          producer.close()
        }
      } else if (tx.compareAndSet(null, transaction)) {
        producer.initTransactions()
      } else {
        throw new IllegalStateException(
          s"State is already associated with transaction ${tx.get} and can't be associated with $transaction")
      }
    }

    override def commit(): Unit = {
      if (inTransaction) {
        inTransaction = false
        producer.commitTransaction()
      }
    }

    override def rollback(): Unit = {
      if (inTransaction) {
        inTransaction = false
        producer.abortTransaction()
      }
    }

    override def addAuthorization(authID: String): Unit = {}
  }
}
