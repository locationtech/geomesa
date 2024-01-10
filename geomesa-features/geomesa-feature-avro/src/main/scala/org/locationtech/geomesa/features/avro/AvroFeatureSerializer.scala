/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.io._
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.serialization.{SimpleFeatureDatumReader, SimpleFeatureDatumWriter}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}

object AvroFeatureSerializer {

  private val encoderFactory = EncoderFactory.get()
  private val decoderFactory = DecoderFactory.get()

  private val outputs = new SoftThreadLocal[ByteArrayOutputStream]()

  private val encoders = new SoftThreadLocal[BinaryEncoder]()
  private val decoders = new SoftThreadLocal[BinaryDecoder]()

  def builder(sft: SimpleFeatureType): Builder = new Builder(sft)

  // Encode using a direct binary encoder that is reused. No need to buffer small simple features
  private def encoder(out: OutputStream): BinaryEncoder = {
    val result = encoderFactory.directBinaryEncoder(out, encoders.get.orNull)
    encoders.put(result)
    result
  }

  private def decoder(in: InputStream): BinaryDecoder = {
    val result = decoderFactory.directBinaryDecoder(in, decoders.get.orNull)
    decoders.put(result)
    result
  }

  private def decoder(in: Array[Byte], offset: Int, length: Int): BinaryDecoder = {
    val result = decoderFactory.binaryDecoder(in, offset, length, decoders.get.orNull)
    decoders.put(result)
    result
  }

  class Builder private [AvroFeatureSerializer] (sft: SimpleFeatureType)
      extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): AvroFeatureSerializer = new AvroFeatureSerializer(sft, options.toSet)
  }
}

/**
 * @param sft the simple feature type to encode
 * @param options the options to apply when encoding
 */
class AvroFeatureSerializer(sft: SimpleFeatureType, val options: Set[SerializationOption] = Set.empty)
    extends SimpleFeatureSerializer {

  private val writer = new SimpleFeatureDatumWriter(sft, options)
  private val reader = SimpleFeatureDatumReader(writer.getSchema, sft)

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    val out = AvroFeatureSerializer.outputs.getOrElseUpdate(new ByteArrayOutputStream())
    out.reset()
    serialize(feature, out)
    out.toByteArray
  }

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit = {
    val encoder = AvroFeatureSerializer.encoder(out)
    writer.write(feature, encoder)
    encoder.flush()
  }

  override def deserialize(in: InputStream): SimpleFeature = reader.read(null, AvroFeatureSerializer.decoder(in))

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize(bytes, 0, bytes.length)

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    reader.read(null, AvroFeatureSerializer.decoder(bytes, offset, length))

  override def deserialize(id: String, in: InputStream): SimpleFeature = {
    val feature = deserialize(in)
    feature.asInstanceOf[ScalaSimpleFeature].setId(id) // TODO cast??
    feature
  }

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val feature = deserialize(bytes, offset, length)
    feature.asInstanceOf[ScalaSimpleFeature].setId(id) // TODO cast??
    feature
  }
}

