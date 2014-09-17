/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, InputStream}

import org.apache.accumulo.core.data.{Value => AValue}
import org.apache.avro.io._
import org.geotools.data.DataUtilities
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.{AvroSimpleFeatureWriter, FeatureSpecificReader}
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


/**
 * Responsible for collecting data-entries that share an identifier, and
 * when done, collapsing these rows into a single SimpleFeature.
 *
 * All encoding/decoding/serializing of features should be done through this
 * single class to allow future versions of serialization instead of scattering
 * knowledge of how the serialization is done through geomesa codebase.
 *
 * A SimpleFeatureEncoder is bound to a given SimpleFeatureType since serialization
 * may depend upon the schema of the feature type.
 *
 * SimpleFeatureEncoder classes may not be thread safe and should generally be used
 * as instance variables for performance reasons. They can serialize and deserialize
 * multiple features.
 */
trait SimpleFeatureEncoder {
  def encode(feature: SimpleFeature) : Array[Byte]
  def decode(featureValue: AValue) : SimpleFeature
  def extractFeatureId(value: AValue): String
  def getName = getEncoding.toString
  def getEncoding: FeatureEncoding
}

object FeatureEncoding extends Enumeration {
  type FeatureEncoding = Value
  val AVRO = Value("avro")
  val TEXT = Value("text")
}

class TextFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder{
  def encode(feature:SimpleFeature) : Array[Byte] =
    ThreadSafeDataUtilities.encodeFeature(feature).getBytes()

  def decode(featureValue: AValue) = {
    ThreadSafeDataUtilities.createFeature(sft, featureValue.toString)
  }

  // This is derived from the knowledge of the GeoTools encoding in DataUtilities
  def extractFeatureId(value: AValue): String = {
    val vString = value.toString
    vString.substring(0, vString.indexOf("="))
  }

  override def getEncoding: FeatureEncoding = FeatureEncoding.TEXT
}

/**
 * This could be done more cleanly, but the object pool infrastructure already
 * existed, so it was quickest, easiest simply to abuse it.
 */
object ThreadSafeDataUtilities {
  private[this] val dataUtilitiesPool = ObjectPoolFactory(new Object, 1)

  def encodeFeature(feature:SimpleFeature) : String = dataUtilitiesPool.withResource {
    _ => DataUtilities.encodeFeature(feature)
  }

  def createFeature(simpleFeatureType:SimpleFeatureType, featureString:String) : SimpleFeature =
    dataUtilitiesPool.withResource {
      _ => DataUtilities.createFeature(simpleFeatureType, featureString)
    }
}

/**
 * Encode features as avro making reuse of binary decoders and encoders
 * as well as a custom datum writer and reader
 *
 * This class is NOT threadsafe and cannot be shared across multiple threads.
 *
 * @param sft
 */
class AvroFeatureEncoder(sft: SimpleFeatureType) extends SimpleFeatureEncoder {

  private val writer = new AvroSimpleFeatureWriter(sft)
  private val reader = FeatureSpecificReader(sft)

  // Encode using a direct binary encoder that is reused. No need to buffer
  // small simple features. Reuse a common BAOS as well.
  private val baos = new ByteArrayOutputStream()
  private var reusableEncoder: DirectBinaryEncoder = null
  def encode(feature: SimpleFeature): Array[Byte] = {
    baos.reset()
    reusableEncoder = EncoderFactory.get().directBinaryEncoder(baos, reusableEncoder).asInstanceOf[DirectBinaryEncoder]
    writer.write(feature, reusableEncoder)
    reusableEncoder.flush()
    baos.toByteArray
  }

  def decode(featureAValue: AValue) = decode(new ByteArrayInputStream(featureAValue.get()))

  // Use a direct binary encoder that is reused. No need to buffer simple features
  // since they are small and no stream read-ahead is required
  private var reusableDecoder: BinaryDecoder = null
  def decode(is: InputStream) = {
    reusableDecoder = DecoderFactory.get().directBinaryDecoder(is, reusableDecoder)
    reader.read(null, reusableDecoder)
  }

  def extractFeatureId(aValue: AValue) =
    FeatureSpecificReader.extractId(new ByteArrayInputStream(aValue.get()), reusableDecoder)

  override def getEncoding: FeatureEncoding = FeatureEncoding.AVRO
}

