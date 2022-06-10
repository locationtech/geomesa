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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 57d901cb17 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 8d7834ec11 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))

  override def serialize(msg: GeoMessage): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    throw new NotImplementedError("Confluent data store is read-only")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> d657014c83 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 8d7834ec11 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a13604e7 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 57d901cb17 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))

  override def serialize(msg: GeoMessage): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    throw new NotImplementedError("Confluent data store is read-only")
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> b62770d74c (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> d657014c83 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a9b549cd7b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 5e44b1cc0a (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 1b8cbf843d (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7258020868 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 73f3a8cb69 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> f06b6e106b (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 0b3e844fc4 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 394f5312e0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 8d7834ec11 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 57d901cb17 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 4ae16a2980 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> e9c0143a6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> b09307f5c0 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> db5f86a9db (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de758f45a6 (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 03f3e71921 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 2686de8d09 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
>>>>>>> 1c64e1adc3 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> 5e44b1cc0a (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 030cd33877 (GEOMESA-3198 Kafka streams integration (#2854))
<<<<<<< HEAD
>>>>>>> cebe144269 (GEOMESA-3198 Kafka streams integration (#2854))
=======
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 937ea7115b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e2a2dd4c2e (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 3e82fa518d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e792102d4b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 5545d8b545 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c9a6fc453c (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> 30cbc0fe6d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f9df175e9b (GEOMESA-3198 Kafka streams integration (#2854))
>>>>>>> f6bb7b3744 (GEOMESA-3198 Kafka streams integration (#2854))

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
