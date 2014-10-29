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

package org.locationtech.geomesa.core.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import org.apache.accumulo.core.data.{Value => AValue}
import org.apache.avro.io._
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.{AvroSimpleFeatureFactory, AvroSimpleFeatureWriter, FeatureSpecificReader}
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


trait HasEncoding {
  def encoding: FeatureEncoding
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
trait SimpleFeatureEncoder extends HasEncoding {
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
trait SimpleFeatureDecoder extends HasEncoding {
  def decode(featureValue: AValue): SimpleFeature = decode(featureValue.get)
  def decode(featureBytes: Array[Byte]): SimpleFeature
  def extractFeatureId(value: AValue): String = extractFeatureId(value.get)
  def extractFeatureId(bytes: Array[Byte]): String
}

object SimpleFeatureDecoder {
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding) =
    encoding match {
      case FeatureEncoding.AVRO => new AvroFeatureDecoder(sft)
      case FeatureEncoding.TEXT => new TextFeatureDecoder(sft)
    }

  def apply(originalSft: SimpleFeatureType,
            projectedSft: SimpleFeatureType,
            encoding: FeatureEncoding) =
    encoding match {
      case FeatureEncoding.AVRO => new ProjectingAvroFeatureDecoder(originalSft, projectedSft)
      case FeatureEncoding.TEXT => new ProjectingTextDecoder(originalSft, projectedSft)
    }

  def apply(sft: SimpleFeatureType, encoding: String): SimpleFeatureDecoder =
    SimpleFeatureDecoder(sft, FeatureEncoding.withName(encoding))

  def apply(originalSft: SimpleFeatureType,
            projectedSft: SimpleFeatureType,
            encoding: String): SimpleFeatureDecoder =
    SimpleFeatureDecoder(originalSft, projectedSft, FeatureEncoding.withName(encoding))
}

object SimpleFeatureEncoder {
  def apply(sft: SimpleFeatureType, encoding: FeatureEncoding) =
    encoding match {
      case FeatureEncoding.AVRO => new AvroFeatureEncoder(sft)
      case FeatureEncoding.TEXT => new TextFeatureEncoder(sft)
    }

  def apply(sft: SimpleFeatureType, encoding: String): SimpleFeatureEncoder =
    SimpleFeatureEncoder(sft, FeatureEncoding.withName(encoding))
}

object FeatureEncoding extends Enumeration {
  type FeatureEncoding = Value
  val AVRO = Value("avro")
  val TEXT = Value("text")
}

class TextFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder {
  override def encode(feature:SimpleFeature): Array[Byte] =
    ThreadSafeDataUtilities.encodeFeature(feature).getBytes

  override def encoding: FeatureEncoding = FeatureEncoding.TEXT
}

class TextFeatureDecoder(sft: SimpleFeatureType) extends SimpleFeatureDecoder {
  override def decode(bytes: Array[Byte]) =
    ThreadSafeDataUtilities.createFeature(sft, new String(bytes))

  // This is derived from the knowledge of the GeoTools encoding in DataUtilities
  override def extractFeatureId(bytes: Array[Byte]): String = {
    val featureString = new String(bytes)
    featureString.substring(0, featureString.indexOf("="))
  }

  override def encoding: FeatureEncoding = FeatureEncoding.TEXT
}

class ProjectingTextDecoder(original: SimpleFeatureType, projected: SimpleFeatureType)
  extends TextFeatureDecoder(original) {

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

class AvroFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder {

  private val writer = new AvroSimpleFeatureWriter(sft)

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

class ProjectingAvroFeatureDecoder(original: SimpleFeatureType, projected: SimpleFeatureType)
  extends SimpleFeatureDecoder {

  private val reader = new FeatureSpecificReader(original, projected)

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

class AvroFeatureDecoder(sft: SimpleFeatureType) extends ProjectingAvroFeatureDecoder(sft, sft)