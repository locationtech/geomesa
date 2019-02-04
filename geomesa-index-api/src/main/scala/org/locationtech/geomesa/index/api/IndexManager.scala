/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.metadata.CachedLazyMetadata
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Manages available indices and versions. @see GeoMesaFeatureIndex
  *
  * @param ds data store
  */
class IndexManager(ds: GeoMesaDataStore[_]) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val expiry = CachedLazyMetadata.Expiry.toDuration.get.toMillis

  private val cache = Caffeine.newBuilder().expireAfterWrite(expiry, TimeUnit.MILLISECONDS).build(
    new CacheLoader[String, (Seq[GeoMesaFeatureIndex[_, _]], Map[String, GeoMesaFeatureIndex[_, _]])]() {
      override def load(key: String): (Seq[GeoMesaFeatureIndex[_, _]], Map[String, GeoMesaFeatureIndex[_, _]]) = {
        val sft = CacheKeyGenerator.restore(key)
        val indices = GeoMesaFeatureIndexFactory.create(ds, sft, sft.getIndices)
        (indices, indices.map(i => (i.identifier, i)).toMap)
      }
    }
  )

  /**
    * Gets configured indices for this sft
    *
    * @param sft simple feature type
    * @param mode read/write mode
    * @return
    */
  def indices(sft: SimpleFeatureType, mode: IndexMode = IndexMode.Any): Seq[GeoMesaFeatureIndex[_, _]] = {
    try {
      cache.get(CacheKeyGenerator.cacheKey(sft))._1.filter(_.mode.supports(mode))
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Invalid indices for simple feature type '${sft.getTypeName}':", e)
    }
  }

  /**
    * Return an index with the specified identifier
    *
    * @param sft simple feature type
    * @param identifier identifier
    * @return
    */
  def index[T, U](sft: SimpleFeatureType, identifier: String, mode: IndexMode = IndexMode.Any): GeoMesaFeatureIndex[T, U] = {
    val idx = cache.get(CacheKeyGenerator.cacheKey(sft))._2.getOrElse(identifier,
      throw new IllegalArgumentException(s"No index exists with identifier '$identifier'"))
    if (idx.mode.supports(mode)) {
      idx.asInstanceOf[GeoMesaFeatureIndex[T, U]]
    } else {
      throw new IllegalArgumentException(s"Index '$identifier' does not support mode $mode")
    }
  }
}
