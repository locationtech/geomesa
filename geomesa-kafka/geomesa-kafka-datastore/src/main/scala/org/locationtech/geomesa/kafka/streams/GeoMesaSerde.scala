/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 133afd3681 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> a7c0500a81 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0452af77a1 (GEOMESA-3198 Kafka streams integration (#2854))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.GeoMesaSerializer
import org.locationtech.geomesa.utils.io.CloseWithLogging
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.GeoMesaSerializer
import org.locationtech.geomesa.utils.io.CloseWithLogging
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.GeoMesaSerializer
import org.locationtech.geomesa.utils.io.CloseWithLogging
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))

/**
 * Serde for reading and writing to GeoMesa Kafka topics
 */
<<<<<<< HEAD
class GeoMesaSerde
    extends Serde[GeoMesaMessage]
        with Serializer[GeoMesaMessage]
        with Deserializer[GeoMesaMessage]
        with HasTopicMetadata {
<<<<<<< HEAD

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
class GeoMesaSerde extends Serde[GeoMesaMessage] with HasTopicMetadata {
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

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

<<<<<<< HEAD
  override def close(): Unit = CloseWithLogging(Option(impl))
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  override def serializer(): Serializer[GeoMesaMessage] = this
  override def deserializer(): Deserializer[GeoMesaMessage] = this

  override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] =
    serializerCache.serializer(topic).serialize(data)

  override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage =
    deserializerCache.serializer(topic).deserialize(data)

  override def close(): Unit = {}
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
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
<<<<<<< HEAD

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def serializer(): Serializer[GeoMesaMessage] = this
    override def deserializer(): Deserializer[GeoMesaMessage] = this
    override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] = serializer.serialize(data)
    override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage = serializer.deserialize(data)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def serializer(): Serializer[GeoMesaMessage] = this
    override def deserializer(): Deserializer[GeoMesaMessage] = this
    override def serialize(topic: String, data: GeoMesaMessage): Array[Byte] = serializer.serialize(data)
    override def deserialize(topic: String, data: Array[Byte]): GeoMesaMessage = serializer.deserialize(data)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
    override def close(): Unit = {}
  }
}
