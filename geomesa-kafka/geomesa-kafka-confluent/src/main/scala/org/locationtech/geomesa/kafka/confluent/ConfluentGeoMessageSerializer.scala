/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import org.apache.avro.Schema
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.GeoMessageSerializerFactory
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageSerializer}

import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.UUID

class ConfluentGeoMessageSerializer(sft: SimpleFeatureType, serializer: ConfluentFeatureSerializer)
    extends GeoMessageSerializer(sft, serializer, null, null, 0) {

  override def deserialize(
      key: Array[Byte],
      value: Array[Byte],
      headers: Map[String, Array[Byte]],
      timestamp: Long): GeoMessage = {
    // support null keys - feature ID will be randomly generated
    val validKey = if (key != null) { key } else { randomKey() }
    // by-pass header and old version checks
    super.deserialize(validKey, value, serializer)
  }

  private def randomKey(): Array[Byte] = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
}

object ConfluentGeoMessageSerializer {

  class ConfluentGeoMessageSerializerFactory(schemaRegistryUrl: URL, schemaOverrides: Map[String, Schema])
      extends GeoMessageSerializerFactory(null) {
    override def apply(sft: SimpleFeatureType): GeoMessageSerializer = {
      val serializer =
        ConfluentFeatureSerializer.builder(sft, schemaRegistryUrl, schemaOverrides.get(sft.getTypeName))
            .withoutId.withUserData.build()
      new ConfluentGeoMessageSerializer(sft, serializer)
    }
  }
}
