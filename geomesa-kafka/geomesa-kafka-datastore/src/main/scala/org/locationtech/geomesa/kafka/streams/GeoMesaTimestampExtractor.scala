/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 133afd3681 (GEOMESA-3198 Kafka streams integration (#2854))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.expression.Expression
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.kafka.index.FeatureStateFactory

import java.util.concurrent.ConcurrentHashMap

object GeoMesaTimestampExtractor {

  /**
   * Get a timestamp extractor based on the data store config
   *
   * @param params data store params
   * @return
   */
  def apply(params: java.util.Map[String, _]): TimestampExtractor = {
    val paramsWithLazyFeatures = new java.util.HashMap[String, Any](params)
    // set lazy features so we don't deserialize the whole message just to get the timestamp
    paramsWithLazyFeatures.put(KafkaDataStoreParams.LazyFeatures.key, "true")
    // disable consumers if not already done
    paramsWithLazyFeatures.put(KafkaDataStoreParams.ConsumerCount.key, 0)

    // check for event time config
    val eventTime = if (!KafkaDataStoreParams.EventTimeOrdering.lookup(params)) { None } else {
      KafkaDataStoreParams.EventTime.lookupOpt(params)
    }
    eventTime match {
      case None => new DefaultDateExtractor(paramsWithLazyFeatures)
      case Some(e) => new EventTimestampExtractor(e, paramsWithLazyFeatures)
    }
  }

  /**
   * Timestamp extractor that will pull the timestamp from the serialized feature
   *
   * @param expression expression for the timestamp
   * @param params data store params
   */
  class EventTimestampExtractor(expression: String, params: java.util.Map[String, _])
      extends FeatureTimestampExtractor(params) {
    override protected def loadExpression(sft: SimpleFeatureType): Option[Expression] =
      Option(FastFilterFactory.toExpression(sft, expression))
  }

  /**
   * Timestamp extractor that will pull the timestamp from the serialized feature
   *
   * @param params data store params
   */
  class DefaultDateExtractor(params: java.util.Map[String, _]) extends FeatureTimestampExtractor(params) {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    override protected def loadExpression(sft: SimpleFeatureType): Option[Expression] =
      sft.getDtgField.map(FastFilterFactory.toExpression(sft, _))
  }

  /**
   * Base class for extracting timestamps from simple features
   *
   * @param params data store params
   */
  private[streams] abstract class FeatureTimestampExtractor(params: java.util.Map[String, _])
      extends TimestampExtractor {

    private val cache = new SerializerCache(params.asInstanceOf[java.util.Map[String, Any]])
    private val expressions = new ConcurrentHashMap[String, Option[Expression]]

    private val expressionLoader = new java.util.function.Function[String, Option[Expression]]() {
      override def apply(topic: String): Option[Expression] =
        loadExpression(cache.serializer(topic).sft)
    }

    // track last-used serializer so we don't have to look them up by hash each
    // time if we're just reading/writing to one topic (which is the standard use-case)
    @volatile
    private var last: (String, Option[Expression]) = ("", null)

    protected def loadExpression(sft: SimpleFeatureType): Option[Expression]

    override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
      record.value() match {
        case null =>
          record.timestamp()

        case m: GeoMesaMessage if m.action == MessageAction.Upsert =>
          val topic = record.topic()
          val (lastTopic, lastExpression) = last
          val expression = if (lastTopic == topic) { lastExpression } else {
            val expression = expressions.computeIfAbsent(topic, expressionLoader)
            // should be thread-safe due to volatile
            last = (topic, expression)
            expression
          }
          expression match {
            case None => record.timestamp()
            case Some(e) => FeatureStateFactory.time(e, cache.serializer(topic).wrap(m))
          }

        case m: GeoMesaMessage if m.action == MessageAction.Delete =>
          record.timestamp()

        case _: GeoMesaMessage =>
          throw new NotImplementedError() // if we forget to handle a message action

        case v =>
          throw new IllegalArgumentException(s"Expected a GeoMesaMessage but got: ${v.getClass.getName} $v")
      }
    }
  }
}
