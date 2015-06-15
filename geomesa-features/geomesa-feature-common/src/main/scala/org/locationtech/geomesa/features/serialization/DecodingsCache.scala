/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
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