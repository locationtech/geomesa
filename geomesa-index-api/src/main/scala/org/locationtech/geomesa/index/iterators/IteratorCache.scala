/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaFeatureIndexFactory}
import org.locationtech.geomesa.utils.cache.ThreadLocalCache
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Cache for expensive objects used in iterators
  */
object IteratorCache extends StrictLogging {

  // force evaluation of accessor cache while setting the context classloader to pick up our custm accessors
  org.locationtech.geomesa.utils.geotools.SimpleFeaturePropertyAccessor.initialize()

  private val expiry = SystemProperty("geomesa.filter.remote.cache.expiry", "10 minutes").toDuration.get

  // thread safe object caches
  private val serializerCache: Cache[(String, String), KryoFeatureSerializer] =
    Caffeine.newBuilder().expireAfterAccess(expiry.toMillis, TimeUnit.MILLISECONDS).build()
  private val indexCache: Cache[(String, String), GeoMesaFeatureIndex[_, _]] =
    Caffeine.newBuilder().expireAfterAccess(expiry.toMillis, TimeUnit.MILLISECONDS).build()

  // non-thread safe object caches
  private val filterCache = new ThreadLocalCache[(String, String), Filter](expiry)

  /**
    * Returns a cached simple feature type, creating one if necessary. Note: do not modify returned value.
    *
    * @param spec simple feature type spec
    * @return
    */
  def sft(spec: String): SimpleFeatureType = SimpleFeatureTypes.createImmutableType("", spec)

  /**
    * Returns a cached serializer, creating one if necessary
    *
    * @param spec simple feature type spec
    * @param options serialization options
    * @return
    */
  def serializer(spec: String, options: Set[SerializationOption]): KryoFeatureSerializer = {
    // note: before the cache is populated, we might end up creating multiple objects, but it is still thread-safe
    val cached = serializerCache.getIfPresent((spec, options.mkString))
    if (cached != null) { cached } else {
      val serializer = KryoFeatureSerializer(sft(spec), options)
      serializerCache.put((spec, options.mkString), serializer)
      serializer
    }
  }

  /**
    * Returns a cached filter, creating one if necessary.
    *
    * Note: need to include simple feature type in cache key,
    * as attribute name -> attribute index gets cached in the filter
    *
    * @param sft simple feature type being filtered
    * @param spec spec string for the simple feature type
    * @param ecql ecql
    * @return
    */
  def filter(sft: SimpleFeatureType, spec: String, ecql: String): Filter = {
    logger.debug(s"Filter cache estimated size: ${filterCache.estimatedGlobalSize}")
    logger.trace(s"Filter cache entries: ${filterCache.globalIterator.map { case (t, (k, v), _) => s"thread $t $k=>$v" }.mkString(", ")}")
    filterCache.getOrElseUpdate((spec, ecql), FastFilterFactory.toFilter(sft, ecql))
  }

  /**
    * Gets a cached feature index instance. Note that the index is not backed by a data store as
    * normal, so operations which require a live connection will fail
    *
    * @param sft simple feature type
    * @param spec spec string for the simple feature type
    * @param identifier index id
    * @return
    */
  def index(sft: SimpleFeatureType, spec: String, identifier: String): GeoMesaFeatureIndex[_, _] = {
    val cached = indexCache.getIfPresent((spec, identifier))
    if (cached != null) { cached } else {
      val index = GeoMesaFeatureIndexFactory.create(null, sft, Seq(IndexId.id(identifier))).headOption.getOrElse {
        throw new RuntimeException(s"Index option not configured correctly: $identifier")
      }
      if (!index.mode.supports(IndexMode.Read)) {
        throw new RuntimeException(s"Index option configured for a non-readable index: $identifier")
      }
      indexCache.put((spec, identifier), index)
      index
    }
  }
}
