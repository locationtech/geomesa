/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.time.Duration

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.locationtech.geomesa.kafka.RecordVersions
import org.locationtech.geomesa.kafka.consumer.BatchConsumer

import scala.util.control.NonFatal

/**
 * Message processor class. Guarantees 'at-least-once' processing.
 */
trait GeoMessageProcessor {

  /**
   * Consume a batch of records.
   *
   * If this method returns true, the batch is considered complete. If it returns false, the batch
   * is considered in error, and will be retried at a later point.
   *
   * Note: if there is an error committing the batch or something else goes wrong, some messages may
   * be repeated in a subsequent call, even if this method returns true.
   *
   * @param records records
   * @return true if the batch was processed successfully, false otherwise
   */
  def consume(records: Seq[GeoMessage]): Boolean
}

object GeoMessageProcessor {

  /**
   * Kafka message consumer with guaranteed at-least-once processing
   *
   * @param consumers consumers
   * @param frequency frequency
   * @param serializer serializer
   * @param processor message processor
   */
  class GeoMessageConsumer(
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      frequency: Duration,
      serializer: GeoMessageSerializer,
      processor: GeoMessageProcessor
    ) extends BatchConsumer(consumers, frequency) {

    override protected def consume(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): Boolean = {
      val messages = records.flatMap { record =>
        try {
          val headers = RecordVersions.getHeaders(record)
          val timestamp = RecordVersions.getTimestamp(record)
          Iterator.single(serializer.deserialize(record.key, record.value, headers, timestamp))
        } catch {
          case NonFatal(e) => logger.error("Error deserializing message:", e); Iterator.empty
        }
      }
      if (messages.isEmpty) {
        true
      } else {
        processor.consume(messages)
      }
    }
  }
}
