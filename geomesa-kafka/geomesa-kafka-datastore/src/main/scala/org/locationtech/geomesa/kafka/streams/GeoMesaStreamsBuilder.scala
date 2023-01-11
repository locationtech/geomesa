/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory

import scala.concurrent.duration.Duration

/**
 * Wrapper for a kafka streams builder that will configure serialization based on a GeoMesa Kafka feature store
 *
 * @param wrapped streams builder
 * @param serde serialization for geomesa messages
 * @param timestampExtractor timestamp extractor
 * @param resetPolicy reset policy
 */
class GeoMesaStreamsBuilder(
    val wrapped: StreamsBuilder,
    serde: GeoMesaSerde,
    timestampExtractor: TimestampExtractor,
    resetPolicy: Option[AutoOffsetReset]) {

  import org.apache.kafka.streams.scala.Serdes.String

  private implicit val s: GeoMesaSerde = serde

  implicit val consumed: Consumed[String, GeoMesaMessage] = resetPolicy match {
    case None => Consumed.`with`(timestampExtractor)
    case Some(p) => Consumed.`with`(timestampExtractor, p)
  }

  /**
   * Gets a serde for the given feature type
   *
   * @param typeName feature type name
   * @return
   */
  def serde(typeName: String): Serde[GeoMesaMessage] = serde.forType(typeName)

  /**
   * Create a stream of updates for a given feature type
   *
   * @param typeName feature type name
   * @return
   */
  def stream(typeName: String): KStream[String, GeoMesaMessage] = wrapped.stream(serde.topic(typeName))

  /**
   * Create a table for a given feature type
   *
   * @param typeName feature type name
   * @return
   */
  def table(typeName: String): KTable[String, GeoMesaMessage] = wrapped.table(serde.topic(typeName))

  /**
   * Create a table for a given feature type
   *
   * @param typeName feature type name
   * @param materialized materialized
   * @return
   */
  def table(
      typeName: String,
      materialized: Materialized[String, GeoMesaMessage, ByteArrayKeyValueStore]): KTable[String, GeoMesaMessage] =
    wrapped.table(serde.topic(typeName), materialized)

  /**
   * Create a global table for a given feature type
   *
   * @param typeName feature type name
   * @return
   */
  def globalTable(typeName: String): GlobalKTable[String, GeoMesaMessage] =
    wrapped.globalTable(serde.topic(typeName))

  /**
   * Create a global table for a given feature type
   *
   * @param typeName feature type name
   * @param materialized materialized
   * @return
   */
  def globalTable(
      typeName: String,
      materialized: Materialized[String, GeoMesaMessage, ByteArrayKeyValueStore]): GlobalKTable[String, GeoMesaMessage] =
    wrapped.globalTable(serde.topic(typeName), materialized)

  /**
   * Write the stream to the given feature type, which must already exist. The messages
   * must conform to the feature type schema
   *
   * @param typeName feature type name
   * @param stream stream to persist
   */
  def to(typeName: String, stream: KStream[String, GeoMesaMessage]): Unit = {
    implicit val produced: Produced[String, GeoMesaMessage] =
      if (serde.usesDefaultPartitioning(typeName)) {
        Produced.`with`
      } else {
        Produced.`with`(new GeoMessageStreamPartitioner())
      }
    stream.to(serde.topic(typeName))
  }

  /**
   * Convenience method to build the underlying topology
   *
   * @return
   */
  def build(): Topology = wrapped.build()
}

object GeoMesaStreamsBuilder {

  import scala.collection.JavaConverters._

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @return
   */
  def apply(params: Map[String, String]): GeoMesaStreamsBuilder =
    apply(params, null, null, null)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param streamsBuilder underlying streams builder to use
   * @return
   */
  def apply(
      params: Map[String, String],
      streamsBuilder: StreamsBuilder): GeoMesaStreamsBuilder =
    apply(params, null, null, streamsBuilder)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param timestampExtractor timestamp extractor for message stream
   * @return
   */
  def apply(
      params: Map[String, String],
      timestampExtractor: TimestampExtractor): GeoMesaStreamsBuilder =
    apply(params, timestampExtractor, null, null)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param timestampExtractor timestamp extractor for message stream
   * @param streamsBuilder underlying streams builder to use
   * @return
   */
  def apply(
      params: Map[String, String],
      timestampExtractor: TimestampExtractor,
      streamsBuilder: StreamsBuilder): GeoMesaStreamsBuilder =
    apply(params, timestampExtractor, null, streamsBuilder)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param resetPolicy auto offset reset for reading existing topics
   * @return
   */
  def apply(
      params: Map[String, String],
      resetPolicy: AutoOffsetReset): GeoMesaStreamsBuilder =
    apply(params, null, resetPolicy, null)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param resetPolicy auto offset reset for reading existing topics
   * @param streamsBuilder underlying streams builder to use
   * @return
   */
  def apply(
      params: Map[String, String],
      resetPolicy: AutoOffsetReset,
      streamsBuilder: StreamsBuilder): GeoMesaStreamsBuilder =
    apply(params, null, resetPolicy, streamsBuilder)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param timestampExtractor timestamp extractor for message stream
   * @param resetPolicy auto offset reset for reading existing topics
   * @return
   */
  def apply(
      params: Map[String, String],
      timestampExtractor: TimestampExtractor,
      resetPolicy: AutoOffsetReset): GeoMesaStreamsBuilder =
    apply(params, timestampExtractor, resetPolicy, null)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param timestampExtractor timestamp extractor for message stream
   * @param resetPolicy auto offset reset for reading existing topics
   * @param streamsBuilder underlying streams builder to use
   * @return
   */
  def apply(
      params: Map[String, String],
      timestampExtractor: TimestampExtractor,
      resetPolicy: AutoOffsetReset,
      streamsBuilder: StreamsBuilder): GeoMesaStreamsBuilder =
    apply(params.asJava, timestampExtractor, resetPolicy, streamsBuilder)

  /**
   * Create a streams builder
   *
   * @param params data store parameters
   * @param timestampExtractor timestamp extractor for message stream
   * @param resetPolicy auto offset reset for reading existing topics
   * @param streamsBuilder underlying streams builder to use
   * @return
   */
  def apply(
      params: java.util.Map[String, String],
      timestampExtractor: TimestampExtractor,
      resetPolicy: AutoOffsetReset,
      streamsBuilder: StreamsBuilder): GeoMesaStreamsBuilder = {
    val serde = new GeoMesaSerde()
    serde.configure(params, isKey = false)
    val builder = Option(streamsBuilder).getOrElse(new StreamsBuilder())
    val timestamps = Option(timestampExtractor).getOrElse(GeoMesaTimestampExtractor(params))
    val reset = Option(resetPolicy).orElse(resetConfig(params))
    new GeoMesaStreamsBuilder(builder, serde, timestamps, reset)
  }

  private def resetConfig(params: java.util.Map[String, String]): Option[AutoOffsetReset] = {
    val config = KafkaDataStoreFactory.buildConfig(params.asInstanceOf[java.util.Map[String, java.io.Serializable]])
    config.consumers.properties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).map {
      case r if r.equalsIgnoreCase(AutoOffsetReset.EARLIEST.name()) => AutoOffsetReset.EARLIEST
      case r if r.equalsIgnoreCase(AutoOffsetReset.LATEST.name()) => AutoOffsetReset.LATEST
      case r => throw new IllegalArgumentException(s"Invalid ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}: $r")
    }.orElse {
      config.consumers.readBack.collect { case Duration.Inf => AutoOffsetReset.EARLIEST }
    }
  }
}
