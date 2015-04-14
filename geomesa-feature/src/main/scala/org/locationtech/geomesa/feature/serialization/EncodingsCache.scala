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

import org.locationtech.geomesa.feature.serialization.CacheKeyGenerator.cacheKeyForSFT
import org.locationtech.geomesa.utils.cache.{SoftThreadLocal, SoftThreadLocalCache}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/** Caches the encodings for [[SimpleFeatureType]]s.  Each thread has its own cache.
  *
  * Concrete subclasses must be objects not classes.
  */
trait EncodingsCache[Writer] {

  type AttributeEncoding = (Writer, SimpleFeature) => Unit

  private val writersCache = new SoftThreadLocal[AbstractWriter[Writer]]()
  private val encodingsCache = new SoftThreadLocalCache[String, Array[AttributeEncoding]]()

  protected def datumWritersFactory: () => AbstractWriter[Writer]

  /**
   * @return a [[AbstractWriter[Writer]] created lazily and cached one per thread
   */
  def writer: AbstractWriter[Writer] = {
    writersCache.getOrElseUpdate(datumWritersFactory())
  }

  /** Gets a sequence of functions to encode the attributes of a simple feature.  Cached per thread
    * and [[SimpleFeatureType]].
    */
  def encodings(sft: SimpleFeatureType): Array[(Writer, SimpleFeature) => Unit] =
    encodingsCache.getOrElseUpdate(cacheKeyForSFT(sft), {

      val datumWriters = writer

      sft.getAttributeDescriptors.zipWithIndex.toArray.map { case (d, i) =>

        // This method is needed to capture the type of the attribute in order to type the writer.
        @inline def encode[T](attribClass: Class[T]): AttributeEncoding = {
          val attribWriter: DatumWriter[Writer, T] =
            datumWriters.selectWriter(attribClass, d.getUserData, datumWriters.standardNullable)
          (writer: Writer, sf: SimpleFeature) => attribWriter(writer, sf.getAttribute(i).asInstanceOf[T])
        }

        encode(d.getType.getBinding)
      }
    })
}
