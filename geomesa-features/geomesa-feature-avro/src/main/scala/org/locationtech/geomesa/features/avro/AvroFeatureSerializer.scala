/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import org.apache.avro.io.{BinaryDecoder, DecoderFactory, DirectBinaryEncoder, EncoderFactory}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SimpleFeatureDeserializer, SimpleFeatureSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


/**
 * @param sft the simple feature type to encode
 * @param options the options to apply when encoding
 */
class AvroFeatureSerializer(sft: SimpleFeatureType, val options: SerializationOptions = SerializationOptions.none)
    extends SimpleFeatureSerializer {

  private val writer = new AvroSimpleFeatureWriter(sft, options)

  // Encode using a direct binary encoder that is reused. No need to buffer
  // small simple features. Reuse a common BAOS as well.
  private val baos = new ByteArrayOutputStream()
  private var reuse: DirectBinaryEncoder = null

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    baos.reset()
    reuse = EncoderFactory.get().directBinaryEncoder(baos, reuse).asInstanceOf[DirectBinaryEncoder]
    writer.write(feature, reuse)
    reuse.flush()
    baos.toByteArray
  }
}

/**
 * @param original the simple feature type that was encoded
 * @param projected the simple feature type to project to when decoding
 * @param options the options what were applied when encoding
 */
class ProjectingAvroFeatureDeserializer(original: SimpleFeatureType, projected: SimpleFeatureType,
                                   val options: SerializationOptions = SerializationOptions.none)
    extends SimpleFeatureDeserializer {

  private val reader = new FeatureSpecificReader(original, projected, options)

  override def deserialize(bytes: Array[Byte]): SimpleFeature = decode(new ByteArrayInputStream(bytes))

  private var reuse: BinaryDecoder = null

  def decode(is: InputStream) = {
    reuse = DecoderFactory.get().directBinaryDecoder(is, reuse)
    reader.read(null, reuse)
  }
}

/**
 * @param sft the simple feature type to decode
 * @param options the options what were applied when encoding
 */
class AvroFeatureDeserializer(sft: SimpleFeatureType, options: SerializationOptions = SerializationOptions.none)
    extends ProjectingAvroFeatureDeserializer(sft, sft, options)

