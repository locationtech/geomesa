/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.core.index
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.serialization.CacheKeyGenerator
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Encode and decode index values
 */
trait IndexValueEncoder {

  /**
   * Encodes a simple feature into a byte array. Only the attributes marked for inclusion get encoded.
   *
   * @param sf
   * @return
   */
  def encode(sf: SimpleFeature): Array[Byte]

  /**
   * Decodes a byte array into a simple feature
   *
   * @param value
   * @return
   */
  def decode(value: Array[Byte]): SimpleFeature

  def indexSft: SimpleFeatureType
}

object IndexValueEncoder {

  import scala.collection.JavaConversions._

  private val cache = new SoftThreadLocalCache[String, IndexValueEncoder]()

  def apply(sft: SimpleFeatureType, version: Int): IndexValueEncoder = {
    val key = CacheKeyGenerator.cacheKeyForSFT(sft)
    cache.getOrElseUpdate(key, new IndexValueEncoderImpl(getIndexSft(sft), sft))
  }

  /**
   * Gets a feature type compatible with the stored index value
   *
   * @param sft
   * @return
   */
  protected[index] def getIndexSft(sft: SimpleFeatureType) = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName(sft.getTypeName + "--index")
    builder.setAttributes(getIndexValueAttributes(sft))
    builder.setDefaultGeometry(sft.getGeometryDescriptor.getLocalName)
    builder.setCRS(sft.getCoordinateReferenceSystem)
    val indexSft = builder.buildFeatureType()
    indexSft.getUserData.putAll(sft.getUserData)
    indexSft
  }

  /**
   * Gets the attributes that are stored in the index value
   *
   * @param sft
   * @return
   */
  protected[index] def getIndexValueAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] = {
    val geom = sft.getGeometryDescriptor
    val dtg = index.getDtgFieldName(sft).flatMap(d => sft.getAttributeDescriptors.find(_.getLocalName == d))
    Seq(geom) ++ dtg
  }

  /**
   * Gets the attribute names that are stored in the index value
   *
   * @param sft
   * @return
   */
  def getIndexValueFields(sft: SimpleFeatureType): Seq[String] =
    getIndexValueAttributes(sft).map(_.getLocalName)
}

/**
 * Encoder/decoder for index values. Not thread-safe.
 */
class IndexValueEncoderImpl(val indexSft: SimpleFeatureType, fullSft: SimpleFeatureType) extends IndexValueEncoder {

  private val reusableFeature = new ScalaSimpleFeature("", indexSft)
  private val serializer = new KryoFeatureSerializer(indexSft)
  private val geomIndex = fullSft.indexOf(fullSft.getGeometryDescriptor.getLocalName)
  private val dtgIndex = index.getDtgFieldName(fullSft).map(fullSft.indexOf).getOrElse(-1)
  private val setAttributes = if (dtgIndex == -1) {
    (sf: SimpleFeature) => reusableFeature.setAttribute(0, sf.getAttribute(geomIndex))
  } else {
    (sf: SimpleFeature) => {
      reusableFeature.setAttribute(0, sf.getAttribute(geomIndex))
      reusableFeature.setAttribute(1, sf.getAttribute(dtgIndex))
    }
  }

  override def encode(sf: SimpleFeature): Array[Byte] = {
    setAttributes(sf)
    serializer.serialize(reusableFeature)
  }

  override def decode(bytes: Array[Byte]): SimpleFeature = {
    serializer.deserialize(bytes)
  }
}