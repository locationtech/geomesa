/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.geotools.feature.simple.SimpleFeatureImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.avro._
import org.locationtech.geomesa.features.kryo._
import org.locationtech.geomesa.features.nio.LazySimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


object SimpleFeatureDeserializers {

  /**
   * Decode without projecting.
   *
   * @param sft the encoded simple feature type to be decode
   * @param typ the encoding that was used to encode
   * @param options any options that were used to encode
   * @return a new [[SimpleFeatureSerializer]]
   */
  def apply(sft: SimpleFeatureType, typ: SerializationType, options: Set[SerializationOption] = Set.empty) =
    typ match {
      case SerializationType.KRYO => KryoFeatureSerializer(sft, options)
      case SerializationType.AVRO => new AvroFeatureDeserializer(sft, options)
    }
}

object ProjectingSimpleFeatureDeserializers {

  /**
   * Decode and project.
   *
   * @param originalSft the encoded simple feature type to be decode
   * @param projectedSft the simple feature type to project to
   * @param typ the encoding that was used to encode
   * @param options any options that were used to encode
   * @return a new [[SimpleFeatureSerializer]]
   */
  def apply(originalSft: SimpleFeatureType,
            projectedSft: SimpleFeatureType,
            typ: SerializationType,
            options: Set[SerializationOption] = Set.empty) =
    typ match {
      case SerializationType.KRYO => new ProjectingKryoFeatureDeserializer(originalSft, projectedSft, options)
      case SerializationType.AVRO => new ProjectingAvroFeatureDeserializer(originalSft, projectedSft, options)
    }
}

object SimpleFeatureSerializers {

  val simpleFeatureImpls = Seq(classOf[ScalaSimpleFeature],
                               classOf[KryoBufferSimpleFeature],
                               classOf[LazySimpleFeature],
                               classOf[AvroSimpleFeature],
                               classOf[SimpleFeature],
                               classOf[SimpleFeatureImpl])

  /**
   * @param sft the simple feature type to be encoded
   * @param typ the desired encoding
   * @param options the desired options
   * @return a new [[SimpleFeatureSerializer]]
   */
  def apply(sft: SimpleFeatureType,
            typ: SerializationType,
            options: Set[SerializationOption] = Set.empty): SimpleFeatureSerializer =
    typ match {
      case SerializationType.KRYO => KryoFeatureSerializer(sft, options)
      case SerializationType.AVRO => new AvroFeatureSerializer(sft, options)
    }
}
