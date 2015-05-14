/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import org.apache.avro.io._
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HasEncoding {
  def encoding: FeatureEncoding
}

trait HasEncodingOptions {
  def options: EncodingOptions
}

/**
 * Interface to encode SimpleFeatures with a configurable serialization format.
 *
 * A SimpleFeatureEncoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureEncoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons.
 */
trait SimpleFeatureEncoder extends HasEncoding with HasEncodingOptions {
  def encode(feature: SimpleFeature): Array[Byte]
}

/**
 * Interface to read SimpleFeatures with a configurable serialization format.
 *
 * A SimpleFeatureDecoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureDecoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons.
 */
trait SimpleFeatureDecoder extends HasEncoding with HasEncodingOptions {
  def decode(featureBytes: Array[Byte]): SimpleFeature
  def lazyDecode(featureBytes: Array[Byte], reusableFeature: SimpleFeature = null): SimpleFeature =
    decode(featureBytes)
  def extractFeatureId(bytes: Array[Byte]): String
}

object SimpleFeatureDecoder {

  /**
   * Decode without projecting.
   *
   * @param sft the encoded simple feature type to be decode
   * @param encoding the encoding that was used to encode
   * @param options any options that were used to encode
   * @return a new [[SimpleFeatureDecoder]]
   */
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding, options: EncodingOptions = EncodingOptions.none) =
    encoding match {
      case FeatureEncoding.KRYO => new KryoFeatureDecoder(sft, options)
      case FeatureEncoding.AVRO => new AvroFeatureDecoder(sft, options)
    }
}

object ProjectingSimpleFeatureDecoder {

  /**
   * Decode and project.
   *
   * @param originalSft the encoded simple feature type to be decode
   * @param projectedSft the simple feature type to project to
   * @param encoding the encoding that was used to encode
   * @param options any options that were used to encode
   * @return a new [[SimpleFeatureDecoder]]
   */
  def apply(originalSft: SimpleFeatureType, projectedSft: SimpleFeatureType,
            encoding: FeatureEncoding, options: EncodingOptions = EncodingOptions.none) =

    encoding match {
      case FeatureEncoding.KRYO => new ProjectingKryoFeatureDecoder(originalSft, projectedSft, options)
      case FeatureEncoding.AVRO => new ProjectingAvroFeatureDecoder(originalSft, projectedSft, options)
    }
}

object SimpleFeatureEncoder {

  /**
   * @param sft the simple feature type to be encoded
   * @param encoding the desired encoding
   * @param options the desired options
   * @return a new [[SimpleFeatureEncoder]]
   */
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding,
            options: EncodingOptions = EncodingOptions.none): SimpleFeatureEncoder =

    encoding match {
      case FeatureEncoding.KRYO => new KryoFeatureEncoder(sft, options)
      case FeatureEncoding.AVRO => new AvroFeatureEncoder(sft, options)
    }
}

/**
 * @param sft the simple feature type to encode
 * @param options the options to apply when encoding
 */
class AvroFeatureEncoder(sft: SimpleFeatureType, val options: EncodingOptions = EncodingOptions.none)
  extends SimpleFeatureEncoder {

  private val writer = new AvroSimpleFeatureWriter(sft, options)

  // Encode using a direct binary encoder that is reused. No need to buffer
  // small simple features. Reuse a common BAOS as well.
  private val baos = new ByteArrayOutputStream()
  private var reuse: DirectBinaryEncoder = null

  override def encode(feature: SimpleFeature): Array[Byte] = {
    baos.reset()
    reuse = EncoderFactory.get().directBinaryEncoder(baos, reuse).asInstanceOf[DirectBinaryEncoder]
    writer.write(feature, reuse)
    reuse.flush()
    baos.toByteArray
  }

  override val encoding: FeatureEncoding = FeatureEncoding.AVRO
}

/**
 * @param original the simple feature type that was encoded
 * @param projected the simple feature type to project to when decoding
 * @param options the options what were applied when encoding
 */
class ProjectingAvroFeatureDecoder(original: SimpleFeatureType, projected: SimpleFeatureType,
                                   val options: EncodingOptions = EncodingOptions.none)
  extends SimpleFeatureDecoder {

  private val reader = new FeatureSpecificReader(original, projected, options)

  override def decode(bytes: Array[Byte]): SimpleFeature = decode(new ByteArrayInputStream(bytes))

  private var reuse: BinaryDecoder = null

  def decode(is: InputStream) = {
    reuse = DecoderFactory.get().directBinaryDecoder(is, reuse)
    reader.read(null, reuse)
  }

  override def extractFeatureId(bytes: Array[Byte]): String =
    FeatureSpecificReader.extractId(new ByteArrayInputStream(bytes), reuse)

  override val encoding: FeatureEncoding = FeatureEncoding.AVRO
}

/**
 * @param sft the simple feature type to decode
 * @param options the options what were applied when encoding
 */
class AvroFeatureDecoder(sft: SimpleFeatureType, options: EncodingOptions = EncodingOptions.none)
  extends ProjectingAvroFeatureDecoder(sft, sft, options)

/**
 * @param sft the simple feature type to encode
 * @param options the options to apply when encoding
 */
class KryoFeatureEncoder(sft: SimpleFeatureType, projected: SimpleFeatureType,
                         val options: EncodingOptions = EncodingOptions.none)
    extends SimpleFeatureEncoder with SimpleFeatureDecoder {

  def this(sft: SimpleFeatureType, options: EncodingOptions = EncodingOptions.none) {
    this(sft, sft, options)
  }

  val encoder = KryoFeatureSerializer(sft, projected, options)

  override val encoding = FeatureEncoding.KRYO
  override def encode(feature: SimpleFeature) = encoder.write(feature)
  override def decode(featureBytes: Array[Byte]) = encoder.read(featureBytes)
  override def extractFeatureId(featureBytes: Array[Byte]) = encoder.readId(featureBytes)
}

/**
 * @param sft the simple feature type to decode
 * @param options the options what were applied when encoding
 */
class KryoFeatureDecoder(sft: SimpleFeatureType, options: EncodingOptions = EncodingOptions.none)
  extends KryoFeatureEncoder(sft, options)

/**
 * @param original the simple feature type that was encoded
 * @param projected the simple feature type to project to when decoding
 * @param options the options what were applied when encoding
 */
class ProjectingKryoFeatureDecoder(original: SimpleFeatureType, projected: SimpleFeatureType,
                                   options: EncodingOptions = EncodingOptions.none)
  extends KryoFeatureEncoder(original, projected, options)
