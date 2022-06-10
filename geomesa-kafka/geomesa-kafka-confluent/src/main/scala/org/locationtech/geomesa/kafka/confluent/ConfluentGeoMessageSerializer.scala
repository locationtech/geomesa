/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
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

class ConfluentGeoMessageSerializer(sft: SimpleFeatureType, serializer: ConfluentFeatureSerializer)
    extends GeoMessageSerializer(sft, serializer, null, null, 0) {
<<<<<<< HEAD
=======

  override def serialize(msg: GeoMessage): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    throw new NotImplementedError("Confluent data store is read-only")
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))

  override def deserialize(
      key: Array[Byte],
      value: Array[Byte],
      headers: Map[String, Array[Byte]],
      timestamp: Long): GeoMessage = {
    // by-pass header and old version checks
    super.deserialize(key, value, serializer)
  }
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
