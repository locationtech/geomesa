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
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.feature.EncodingOption.EncodingOption
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HasEncoding {
  def encoding: FeatureEncoding
}

trait HasEncodingOptions {
  def options: Set[EncodingOption]
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
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding, options: EncodingOption*) =
    encoding match {
      case FeatureEncoding.KRYO => new KryoFeatureEncoder(sft, options.toSet)
      case FeatureEncoding.AVRO => new AvroFeatureDecoder(sft, options.toSet)
      case FeatureEncoding.TEXT => new TextFeatureDecoder(sft, options.toSet)
    }

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
            encoding: FeatureEncoding, options: EncodingOption*) =
    encoding match {
      case FeatureEncoding.KRYO => new           KryoFeatureEncoder(originalSft, projectedSft, options.toSet)
      case FeatureEncoding.AVRO => new ProjectingAvroFeatureDecoder(originalSft, projectedSft, options.toSet)
      case FeatureEncoding.TEXT => new ProjectingTextFeatureDecoder(originalSft, projectedSft, options.toSet)
    }

  /**
   * Decode without projecting.
   *
   * @param sft the encoded simple feature type to be decode
   * @param encoding the encoding that was used to encode
   * @param options any options that were used to encode
   * @return a new [[SimpleFeatureDecoder]]
   */
  def apply(sft: SimpleFeatureType, encoding: String, options: EncodingOption*): SimpleFeatureDecoder =
    SimpleFeatureDecoder(sft, FeatureEncoding.withName(encoding))

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
            encoding: String, options: EncodingOption*): SimpleFeatureDecoder =
    SimpleFeatureDecoder(originalSft, projectedSft, FeatureEncoding.withName(encoding))
}

object SimpleFeatureEncoder {

  /**
   * @param sft the simple feature type to be encoded
   * @param encoding the desired encoding
   * @param options the desired options
   * @return a new [[SimpleFeatureEncoder]]
   */
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding, options: EncodingOption*): SimpleFeatureEncoder =
    encoding match {
      case FeatureEncoding.KRYO => new KryoFeatureEncoder(sft, options.toSet)
      case FeatureEncoding.AVRO => new AvroFeatureEncoder(sft, options.toSet)
      case FeatureEncoding.TEXT => new TextFeatureEncoder(sft, options.toSet)
    }

  /**
   * @param sft the simple feature type to be encoded
   * @param encoding the desired encoding
   * @param options the desired options
   * @return a new [[SimpleFeatureEncoder]]
   */
  def apply(sft: SimpleFeatureType, encoding: String, options: EncodingOption*): SimpleFeatureEncoder =
    SimpleFeatureEncoder(sft, FeatureEncoding.withName(encoding))
}

object FeatureEncoding extends Enumeration {
  type FeatureEncoding = Value
  val KRYO = Value("kryo")
  val AVRO = Value("avro")
  val TEXT = Value("text")
}

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object EncodingOption extends Enumeration {
  type EncodingOption = Value

  /**
   * If this [[EncodingOption]] is specified then the security marking associated with the sample feature will be
   * serialized and deserialized.
   */
  val WITH_VISIBILITIES = Value("withVisibilities")
}

object EncodingOptions {

  /**
   * Same as ``Set.empty`` but provides better readability.
   *
   * e.g. ``SimpleFeatureEncoder(sft, FeatureEncoding.AVRO, EncodingOptions.none)``
   */
  val none: Set[EncodingOption] = Set.empty
}

class TextFeatureEncoder(sft: SimpleFeatureType, val options: Set[EncodingOption] = Set.empty)
  extends SimpleFeatureEncoder {

  override def encode(feature:SimpleFeature): Array[Byte] =
    ThreadSafeDataUtilities.encodeFeature(feature).getBytes

  override def encoding: FeatureEncoding = FeatureEncoding.TEXT
}

class TextFeatureDecoder(sft: SimpleFeatureType, val options: Set[EncodingOption] = Set.empty)
  extends SimpleFeatureDecoder {

  override def decode(bytes: Array[Byte]) =
    ThreadSafeDataUtilities.createFeature(sft, new String(bytes))

  // This is derived from the knowledge of the GeoTools encoding in DataUtilities
  override def extractFeatureId(bytes: Array[Byte]): String = {
    val featureString = new String(bytes)
    featureString.substring(0, featureString.indexOf("="))
  }

  override def encoding: FeatureEncoding = FeatureEncoding.TEXT
}

class ProjectingTextFeatureDecoder(original: SimpleFeatureType, projected: SimpleFeatureType,
                                   options: Set[EncodingOption] = Set.empty)
  extends TextFeatureDecoder(original, options) {

  private val fac = AvroSimpleFeatureFactory.featureBuilder(projected)
  private val attrs = DataUtilities.attributeNames(projected)

  override def decode(bytes: Array[Byte]) = {
    val sf = super.decode(bytes)
    fac.reset()
    attrs.foreach { attr => fac.set(attr, sf.getAttribute(attr)) }
    fac.buildFeature(sf.getID)
  }
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature): String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String): SimpleFeature =
    dataUtilitiesPool.withResource {
      _ => DataUtilities.createFeature(simpleFeatureType, featureString)
    }
}

class AvroFeatureEncoder(sft: SimpleFeatureType, val options: Set[EncodingOption] = Set.empty)
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

  override def encoding: FeatureEncoding = FeatureEncoding.AVRO
}

class ProjectingAvroFeatureDecoder(original: SimpleFeatureType, projected: SimpleFeatureType,
                                   val options: Set[EncodingOption] = Set.empty)
  extends SimpleFeatureDecoder {

  private val reader = new FeatureSpecificReader(original, projected, options)

  override def decode(bytes: Array[Byte]) = decode(new ByteArrayInputStream(bytes))

  private var reuse: BinaryDecoder = null

  def decode(is: InputStream) = {
    reuse = DecoderFactory.get().directBinaryDecoder(is, reuse)
    reader.read(null, reuse)
  }

  override def extractFeatureId(bytes: Array[Byte]) =
    FeatureSpecificReader.extractId(new ByteArrayInputStream(bytes), reuse)

  override def encoding: FeatureEncoding = FeatureEncoding.AVRO
}

class AvroFeatureDecoder(sft: SimpleFeatureType, opts: Set[EncodingOption] = Set.empty)
  extends ProjectingAvroFeatureDecoder(sft, sft, opts)

/**
 *
 * @param sft the simple feature type to encode or decode
 * @param projected the projected simple feature type for encoding or decoding
 */
class KryoFeatureEncoder(sft: SimpleFeatureType, projected: SimpleFeatureType,
                         val options: Set[EncodingOption] = Set.empty)
    extends SimpleFeatureEncoder with SimpleFeatureDecoder {

  def this(sft: SimpleFeatureType, options: Set[EncodingOption] = Set.empty) {
    this(sft, sft, options)
  }

  val encoder = KryoFeatureSerializer(sft, projected, options)

  override val encoding = FeatureEncoding.KRYO
  override def encode(feature: SimpleFeature) = encoder.write(feature)
  override def decode(featureBytes: Array[Byte]) = encoder.read(featureBytes)
  override def extractFeatureId(featureBytes: Array[Byte]) = encoder.readId(featureBytes)
}
