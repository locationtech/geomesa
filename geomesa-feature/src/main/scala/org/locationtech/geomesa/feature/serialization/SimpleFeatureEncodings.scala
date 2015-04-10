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
import org.locationtech.geomesa.utils.cache.SoftThreadLocalCache
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.ref.SoftReference

/** Provides access to the encodings for a [[SimpleFeatureType]].
 *
 */
class SimpleFeatureEncodings[Writer](val datumWriters: AbstractWriter[Writer], val sft: SimpleFeatureType) {

  type AttributeEncoding = (Writer, SimpleFeature) => Unit

  /**
   * @return a seq of functions to encode the attributes of simple feature
   */
  lazy val attributeEncodings: Array[AttributeEncoding] =
    sft.getAttributeDescriptors.zipWithIndex.toArray.map { case (d, i) =>

      // This method is needed to capture the type of the attribute in order to type the writer.
      @inline def encode[T](attribClass: Class[T]): AttributeEncoding = {
        val attribWriter: DatumWriter[Writer, T] =
          datumWriters.selectWriter(attribClass, d.getUserData, datumWriters.standardNullable)
        (writer: Writer, sf: SimpleFeature) => attribWriter(writer, attribClass.cast(sf.getAttribute(i)))
      }

      encode(d.getType.getBinding)
    }
}

/** Caches [[SimpleFeatureEncodings]] for multiple [[SimpleFeatureType]]s.
  * Each thread has its own cache.
  *
  * Concrete subclasses must be objects not classes.
  */
trait SimpleFeatureEncodingsCache[Writer] {

  private val writersCache = new ThreadLocal[SoftReference[AbstractWriter[Writer]]]()
  private val encodingsCache = new SoftThreadLocalCache[String, SimpleFeatureEncodings[Writer]]()

  protected  def datumWritersFactory: () => AbstractWriter[Writer]

  /** Gets a sequence of functions to encode the attributes of a simple feature. */
  def get(sft: SimpleFeatureType): SimpleFeatureEncodings[Writer] =
    encodingsCache.getOrElseUpdate(cacheKeyForSFT(sft), {
      new SimpleFeatureEncodings[Writer](getAbstractWriter, sft)
    })
  
  def getAbstractWriter: AbstractWriter[Writer] = {
    Option(writersCache.get()).flatMap(_.get).getOrElse {
      val writer = datumWritersFactory()
      writersCache.set(new SoftReference(writer))
      writer
    }
  }
}
