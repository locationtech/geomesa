/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.{FailOnInvalidTimestamp, TimestampExtractor}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{EventTimeConfig, ExpiryTimeConfig, FilteredExpiryConfig}
import org.locationtech.geomesa.kafka.data.{KafkaDataStoreFactory, KafkaDataStoreParams}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory
import org.opengis.filter.expression.Expression

import java.util.concurrent.ConcurrentHashMap

object GeoMesaTimestampExtractor {

  /**
   * Get a timestamp extractor based on the data store config
   *
   * @param params data store params
   * @return
   */
  def apply(params: java.util.Map[String, _]): TimestampExtractor = {
    val lazyParams = new java.util.HashMap[String, Any](params)
    // set lazy features so we don't deserialize the whole message just to get the timestamp
    lazyParams.put(KafkaDataStoreParams.LazyFeatures.key, "true")
    // disable consumers if not already done
    lazyParams.put(KafkaDataStoreParams.ConsumerCount.key, 0)

    val config = KafkaDataStoreFactory.buildConfig(lazyParams.asInstanceOf[java.util.Map[String, java.io.Serializable]])

    val event: PartialFunction[ExpiryTimeConfig, String] = {
      case EventTimeConfig(_, exp, true) => exp
    }

    val expression = config.indices.expiry match {
      case o if event.isDefinedAt(o) => Some(event.apply(o))
      // all filters use the same event time ordering
      case FilteredExpiryConfig(filters) if event.isDefinedAt(filters.head._2) => Some(event.apply(filters.head._2))
      case _ => None
    }

    expression match {
      case None => new FailOnInvalidTimestamp()
      case Some(e) => new EventTimestampExtractor(e, lazyParams)
    }
  }

  /**
   * Timestamp extractor that will pull the timestamp from the serialized feature
   *
   * @param expression expression for the timestamp
   * @param params data store params
   */
  class EventTimestampExtractor(expression: String, params: java.util.Map[String, _]) extends TimestampExtractor {

    private val cache = new SerializerCache(params.asInstanceOf[java.util.Map[String, Any]])
    private val expressions = new ConcurrentHashMap[String, Expression]

    private val expressionLoader = new java.util.function.Function[String, Expression]() {
      override def apply(topic: String): Expression =
        FastFilterFactory.toExpression(cache.serializer(topic).sft, expression)
    }

    // track last-used serializer so we don't have to look them up by hash each
    // time if we're just reading/writing to one topic (which is the standard use-case)
    @volatile
    private var last: (String, Expression) = ("", null)

    override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
      if (record.value() == null) {
        record.timestamp() // delete
      } else {
        val topic = record.topic()
        val serializer = cache.serializer(topic)
        val (lastTopic, lastExpression) = last
        val expression = if (lastTopic == topic) { lastExpression } else {
          val expression = expressions.computeIfAbsent(topic, expressionLoader)
          // should be thread-safe due to volatile
          last = (topic, expression)
          expression
        }
        FeatureStateFactory.time(expression, serializer.internal.deserialize(record.value().asInstanceOf[Array[Byte]]))
      }
    }
  }
}
