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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======

  override def serialize(msg: GeoMessage): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    throw new NotImplementedError("Confluent data store is read-only")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> d657014c83 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')

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
