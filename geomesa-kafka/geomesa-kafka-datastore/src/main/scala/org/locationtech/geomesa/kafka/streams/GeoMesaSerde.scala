/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams
import org.locationtech.geomesa.kafka.streams.GeoMesaSerde.GeoMesaSerializer
import org.locationtech.geomesa.utils.io.CloseWithLogging

/**
 * Serde for reading and writing to GeoMesa Kafka topics
 */
class GeoMesaSerde extends Serde[GeoMesaMessage] {

  private val impl = new GeoMesaSerializer()

  /**
   * Gets the topic associated with a feature type
   *
   * @param typeName feature type name
   * @return
   */
  def topic(typeName: String): String = impl.topic(typeName)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit =
    impl.configure(configs, isKey)

  override def serializer(): Serializer[GeoMesaMessage] = impl
  override def deserializer(): Deserializer[GeoMesaMessage] = impl

  override def close(): Unit = CloseWithLogging(Option(impl))
}

object GeoMesaSerde {

  class GeoMesaSerializer extends Serializer[GeoMesaMessage] with Deserializer[GeoMesaMessage] {

    // track serialization/deserialization separately to avoid cache thrashing
    private var serializerCache: SerializerCache = _
    private var deserializerCache: SerializerCache = _

    /**
     * Gets the topic associated with a feature type
     *
     * @param typeName feature type name
     * @return
     */
    def topic(typeName: String): String = serializerCache.topic(typeName)

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

    override def close(): Unit = {}
  }
}
