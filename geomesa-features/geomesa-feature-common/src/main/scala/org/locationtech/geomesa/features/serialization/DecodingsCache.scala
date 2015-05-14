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
package org.locationtech.geomesa.features.serialization

import org.locationtech.geomesa.utils.cache.{SoftThreadLocal, SoftThreadLocalCache}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Caches decodings for [[SimpleFeatureType]]s.  Each thread has its own cache.
  *
  * Concrete subclasses must be objects not classes.
  */
trait DecodingsCache[Reader] {

  private val readersCache = new SoftThreadLocal[AbstractReader[Reader]]()
  private val decodingsCache = new SoftThreadLocalCache[String, DecodingsVersionCache[Reader]]()

  protected def datumReadersFactory: () => AbstractReader[Reader]

  /**
   * @return a [[AbstractReader[Reader]] created lazily and cached one per thread
   */
  def reader: AbstractReader[Reader] = {
    readersCache.getOrElseUpdate(datumReadersFactory())
  }

  /** Gets a function from a serialization version number to a sequence of functions to decode the attributes
    * of a simple feature.  The decodings are ached per thread, [[SimpleFeatureType]], and version number.
    */
  def decodings(sft: SimpleFeatureType): DecodingsVersionCache[Reader] = {
    decodingsCache.getOrElseUpdate(CacheKeyGenerator.cacheKeyForSFT(sft), {
      new DecodingsVersionCache[Reader](reader, sft)
    })
  }

}

 class DecodingsVersionCache[Reader](datumReaders: AbstractReader[Reader], sft: SimpleFeatureType) {

  type DecodingsArray = Array[DatumReader[Reader, AnyRef]]

  private val versionCache = new mutable.HashMap[Version, DecodingsArray]

  /**
   * @param version the serialization version number
   * @return a seq of functions to decode the attributes of simple feature for the given ``version``
   */
  def forVersion(version: Version): DecodingsArray =
    versionCache.getOrElseUpdate(version, {
      sft.getAttributeDescriptors.map { d =>
        datumReaders.selectReader(d.getType.getBinding, version, d.getUserData, datumReaders.standardNullable)
      }.toArray}
    )
}

object CacheKeyGenerator {

  import collection.JavaConversions._

  def cacheKeyForSFT(sft: SimpleFeatureType) =
    s"${sft.getName};${sft.getAttributeDescriptors.map(ad => s"${ad.getName.toString}${ad.getType}").mkString(",")}"
}