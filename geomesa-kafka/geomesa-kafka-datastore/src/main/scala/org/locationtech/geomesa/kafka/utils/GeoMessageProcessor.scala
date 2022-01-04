/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult.BatchResult

import scala.util.control.NonFatal

/**
 * Message processor class. Guarantees 'at-least-once' processing.
 */
trait GeoMessageProcessor {

  /**
   * Consume a batch of records.
   *
   * The response from this method will determine the continued processing of messages. If `Commit`
   * is returned, the batch is considered complete and won't be presented again. If `Continue` is
   * returned, the batch will be presented again in the future, and more messages will be read off the topic
   * in the meantime. If `Pause` is returned, the batch will be presented again in the future, but
   * no more messages will be read off the topic in the meantime.
   *
   * This method should return in a reasonable amount of time. If too much time is spent processing
   * messages, consumers may be considered inactive and be dropped from processing. See
   * https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
   *
   * Note: if there is an error committing the batch or something else goes wrong, some messages may
   * be repeated in a subsequent call, regardless of the response from this method
   *
   * @param records records
   * @return indication to continue, pause, or commit
   */
  def consume(records: Seq[GeoMessage]): BatchResult
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

    override protected def consume(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): BatchResult = {
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
        BatchResult.Commit
      } else {
        processor.consume(messages)
      }
    }
  }
}
