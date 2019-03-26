/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.time.Duration

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.WritableFeatureCache
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.MessageTypes

/**
  * Consumes from kakfa and populates the local cache
  *   1. reads offsets stored in zk on startup
  *   2. scheduled repeating - reads features from kafka, adds to in-memory cache
  *   3. listens for offsets change in zk, removes expired features from in-memory cache
  *
  * @param consumers consumers
  * @param topic kafka topic
  * @param frequency kafka poll delay, in milliseconds
  * @param serializer feature serializer
  * @param cache shared state
  */
class KafkaCacheLoader(
    consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
    topic: String,
    frequency: Long,
    serializer: KryoFeatureSerializer,
    cache: WritableFeatureCache) extends ThreadedConsumer(consumers, Duration.ofMillis(frequency)) {

  startConsumers()

  override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    val (time, action) = KafkaStore.deserializeKey(record.key)
    val feature = serializer.deserialize(record.value)
    action match {
      case MessageTypes.Write  => cache.add(feature, record.partition, record.offset, time)
      case MessageTypes.Delete => cache.delete(feature, record.partition, record.offset, time)
      case _ => logger.error(s"Unhandled message type: $action")
    }
  }
}
