/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.SimpleFeatureSerializer.LimitedSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{ByteArrayInputStream, InputStream}

/**
 * @param original the simple feature type that was encoded
 * @param projected the simple feature type to project to when decoding
 * @param options the options what were applied when encoding
 */
@deprecated("Deprecated with no replacement")
class ProjectingAvroFeatureDeserializer(
    original: SimpleFeatureType,
    projected: SimpleFeatureType,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with LimitedSerialization {

  private val reader = FeatureSpecificReader(original, projected, options)

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("This instance only handles deserialization")

  override def deserialize(bytes: Array[Byte]): SimpleFeature = decode(new ByteArrayInputStream(bytes))

  private var reuse: BinaryDecoder = _

  def decode(is: InputStream): SimpleFeature = {
    reuse = DecoderFactory.get().directBinaryDecoder(is, reuse)
    reader.read(null, reuse)
  }
}
