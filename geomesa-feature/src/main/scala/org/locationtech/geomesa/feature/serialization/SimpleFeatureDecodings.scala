/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.feature.serialization

import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.opengis.feature.simple.SimpleFeatureType

import collection.JavaConversions._
import CacheKeyGenerator.cacheKeyForSFT

import scala.collection.mutable
import scala.ref.SoftReference


/** Provides access to the decodings for a [[SimpleFeatureType]].
  *
  */
case class SimpleFeatureDecodings[Reader](datumReaders: AbstractReader[Reader], sft: SimpleFeatureType) {

  type DecodingsArray = Array[DatumReader[Reader, AnyRef]]

  private val versionCache = new mutable.HashMap[Version, DecodingsArray]

  /**
   * @return a seq of functions to decode the attributes of simple feature
   */
  def attributeDecodings(version: Version): DecodingsArray =
    versionCache.getOrElseUpdate(version, {
      sft.getAttributeDescriptors.map { d =>
        datumReaders.selectReader(d.getType.getBinding, version, d.getUserData, datumReaders.standardNullable)
      }.toArray}
    )
}

/** Caches [[SimpleFeatureDecodings]] for multiple [[SimpleFeatureType]]s.
  * Each thread has its own cache.
  *
  * Concrete subclasses must be objects not classes.
  */
trait SimpleFeatureDecodingsCache[Reader] {

  private val readersCache = new ThreadLocal[SoftReference[AbstractReader[Reader]]]()
  private val decodingsCache = new SoftThreadLocalCache[String, SimpleFeatureDecodings[Reader]]()

  protected def datumReadersFactory: () => AbstractReader[Reader]

  /** Gets a sequence of functions to decode the attributes of a simple feature. */
  def get(sft: SimpleFeatureType): SimpleFeatureDecodings[Reader] =
    decodingsCache.getOrElseUpdate(cacheKeyForSFT(sft), {
      new SimpleFeatureDecodings[Reader](getAbstractReader, sft)
    })

  def getAbstractReader: AbstractReader[Reader] = {
    Option(readersCache.get()).flatMap(_.get).getOrElse {
      val reader = datumReadersFactory()
      readersCache.set(new SoftReference(reader))
      reader
    }
  }
}

object CacheKeyGenerator {

  import collection.JavaConversions._

  def cacheKeyForSFT(sft: SimpleFeatureType) =
    s"${sft.getName};${sft.getAttributeDescriptors.map(ad => s"${ad.getName.toString}${ad.getType}").mkString(",")}"
}