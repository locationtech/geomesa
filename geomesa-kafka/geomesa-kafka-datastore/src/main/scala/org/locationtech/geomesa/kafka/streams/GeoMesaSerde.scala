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

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
<<<<<<< HEAD
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.TypeSpecificSerde
=======
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.GeoMesaSerializer
import org.locationtech.geomesa.utils.io.CloseWithLogging
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))

/**
 * Serde for reading and writing to GeoMesa Kafka topics
 */
<<<<<<< HEAD
class GeoMesaSerde
    extends Serde[GeoMesaMessage]
        with Serializer[GeoMesaMessage]
        with Deserializer[GeoMesaMessage]
        with HasTopicMetadata {

  // track serialization/deserialization separately to avoid cache thrashing
  private var serializerCache: SerializerCache = _
  private var deserializerCache: SerializerCache = _

  override def topic(typeName: String): String = serializerCache.topic(typeName)
  override def usesDefaultPartitioning(typeName: String): Boolean =
    serializerCache.usesDefaultPartitioning(typeName)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    require(!isKey, "GeoMesaSerializer does not support key serialization")
    val params = new java.util.HashMap[String, Any](configs)
    // disable consumers if not already done
    params.put(KafkaDataStoreParams.ConsumerCount.key, 0)
    this.serializerCache = new SerializerCache(params)
    this.deserializerCache = new SerializerCache(params)
  }

  /**
   * Gets a serde for the given feature type
   *
   * @param typeName feature type name
   * @return
   */
  def forType(typeName: String): Serde[GeoMesaMessage] =
    new TypeSpecificSerde(serializerCache.serializer(topic(typeName)))

  override def serializer(): Serializer[GeoMesaMessage] = this
  override def deserializer(): Deserializer[GeoMesaMessage] = this

  override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] =
    serializerCache.serializer(topic).serialize(data)

  override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage =
    deserializerCache.serializer(topic).deserialize(data)

  override def close(): Unit = {}
=======
class GeoMesaSerde extends Serde[GeoMesaMessage] with HasTopicMetadata {

  private val impl = new GeoMesaSerializer()

  override def topic(typeName: String): String = impl.topic(typeName)
  override def usesDefaultPartitioning(typeName: String): Boolean = impl.usesDefaultPartitioning(typeName)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
    impl.configure(configs, isKey)

  override def serializer(): Serializer[GeoMesaMessage] = impl
  override def deserializer(): Deserializer[GeoMesaMessage] = impl

  override def close(): Unit = CloseWithLogging(Option(impl))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
}

object GeoMesaSerde {

<<<<<<< HEAD
  /**
   * Serde for a given feature type - does not consider the topic being read
   *
   * @param serializer serializer
   */
  class TypeSpecificSerde(serializer: GeoMesaMessageSerializer)
      extends Serde[GeoMesaMessage]
          with Serializer[GeoMesaMessage]
          with Deserializer[GeoMesaMessage] {

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def serializer(): Serializer[GeoMesaMessage] = this
    override def deserializer(): Deserializer[GeoMesaMessage] = this
    override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] = serializer.serialize(data)
    override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage = serializer.deserialize(data)
=======
  class GeoMesaSerializer extends Serializer[GeoMesaMessage] with Deserializer[GeoMesaMessage] with HasTopicMetadata {

    // track serialization/deserialization separately to avoid cache thrashing
    private var serializerCache: SerializerCache = _
    private var deserializerCache: SerializerCache = _

    override def topic(typeName: String): String = serializerCache.topic(typeName)
    override def usesDefaultPartitioning(typeName: String): Boolean =
      serializerCache.usesDefaultPartitioning(typeName)

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
      require(!isKey, "GeoMesaSerializer does not support key serialization")
      val params = new java.util.HashMap[String, Any](configs)
      // disable consumers if not already done
      params.put(KafkaDataStoreParams.ConsumerCount.key, 0)
      this.serializerCache = new SerializerCache(params)
      this.deserializerCache = new SerializerCache(params)
    }

    override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] =
      serializerCache.serializer(topic).serialize(data)

    override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage =
      deserializerCache.serializer(topic).deserialize(data)

>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
    override def close(): Unit = {}
  }
}
