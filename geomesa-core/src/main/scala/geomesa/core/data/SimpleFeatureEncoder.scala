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

package geomesa.core.data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.google.common.cache.LoadingCache
import geomesa.core.data.FeatureEncoding.FeatureEncoding
import geomesa.feature.{AvroSimpleFeature, FeatureSpecificReader}
import geomesa.utils.text.ObjectPoolFactory
import org.apache.accumulo.core.data.{ Value => AValue }
import org.apache.avro.io.DecoderFactory
import org.geotools.data.DataUtilities
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


/**
 * Responsible for collecting data-entries that share an identifier, and
 * when done, collapsing these rows into a single SimpleFeature.
 *
 * All encoding/decoding/serializing of features should be done through this
 * single class to allow future versions of serialization instead of scattering
 * knowledge of how the serialization is done through the geomesa-core codebase
 */

trait SimpleFeatureEncoder {
  def encode(feature:SimpleFeature) : AValue
  def decode(simpleFeatureType: SimpleFeatureType, featureValue: AValue) : SimpleFeature
  def extractFeatureId(value: AValue): String
  def getName = getEncoding.toString
  def getEncoding: FeatureEncoding
}

object FeatureEncoding extends Enumeration {
  type FeatureEncoding = Value
  val AVRO = Value("avro")
  val TEXT = Value("text")
}

class TextFeatureEncoder extends SimpleFeatureEncoder{
  def encode(feature:SimpleFeature) : AValue =
    new AValue(ThreadSafeDataUtilities.encodeFeature(feature).getBytes)

  def decode(simpleFeatureType: SimpleFeatureType, featureValue: AValue) = {
    ThreadSafeDataUtilities.createFeature(simpleFeatureType, featureValue.toString)
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

// TODO the AvroFeatureEncoder may not be threadsafe...evaluate.
class AvroFeatureEncoder extends SimpleFeatureEncoder {

  def encode(feature: SimpleFeature): AValue = {
    val asf = feature.getClass match {
      case c if classOf[AvroSimpleFeature].isAssignableFrom(c) => feature.asInstanceOf[AvroSimpleFeature]
      case _ =>  AvroSimpleFeature(feature)
    }
    val baos = new ByteArrayOutputStream()
    asf.write(baos)
    new AValue(baos.toByteArray)
  }

  def decode(simpleFeatureType: SimpleFeatureType, featureAValue: AValue) = {
    val bais = new ByteArrayInputStream(featureAValue.get())
    val decoder = DecoderFactory.get().binaryDecoder(bais, null)
    readerCache.get(simpleFeatureType).read(null, decoder)
  }

  def extractFeatureId(aValue: AValue) = FeatureSpecificReader.extractId(new ByteArrayInputStream(aValue.get()))

  val readerCache: LoadingCache[SimpleFeatureType, FeatureSpecificReader] =
    AvroSimpleFeature.loadingCacheBuilder { sft => FeatureSpecificReader(sft) }

  override def getEncoding: FeatureEncoding = FeatureEncoding.AVRO
}

