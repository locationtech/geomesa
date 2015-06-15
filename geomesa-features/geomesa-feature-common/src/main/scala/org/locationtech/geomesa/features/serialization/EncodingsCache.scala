/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.features.serialization

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
    encodingsCache.getOrElseUpdate(CacheKeyGenerator.cacheKeyForSFT(sft), {

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
