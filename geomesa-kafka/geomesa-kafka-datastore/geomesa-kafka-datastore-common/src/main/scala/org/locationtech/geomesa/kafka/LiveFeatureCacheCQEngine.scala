/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache._
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class LiveFeatureCacheCQEngine(sft: SimpleFeatureType,
                               expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends LiveFeatureCache with LazyLogging {

  val geocq = new GeoCQEngine(sft)

  private val cache = {
    val cb = Caffeine.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          override def onRemoval(key: String, value: FeatureHolder, cause: RemovalCause): Unit = {
            if (cause == RemovalCause.EXPIRED) {
              logger.debug(s"Removing feature $key due to expiration after ${ep}ms")
              geocq.remove(value.sf)
            }
          }
        })
    }
    cb.build[String, FeatureHolder]()
  }

  val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  def size(): Int = {
    features.size
  }

  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      SelfClosingIterator(getReaderForFilter(f)).length
    }
  }

  override def cleanUp(): Unit = { cache.cleanUp() }

  /**
    * WARNING: this method is not thread-safe. CQEngine's ConcurrentIndexedCollection
    * does provide some protections on simultaneous mutations, but not if two threads
    * write the same feature.
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = sf.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      geocq.remove(old.sf)
    }
    val env = sf.geometry.getEnvelopeInternal
    geocq.add(sf)
    cache.put(id, FeatureHolder(sf, env))
  }

  override def getFeatureById(id: String): FeatureHolder = cache.getIfPresent(id)

  /**
    * WARNING: this method is not thread-safe. CQEngine's ConcurrentIndexedCollection
    * does provide some protections on simultaneous mutations, but not if two threads
    * write the same feature.
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      geocq.remove(old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    geocq.clear()  // Consider re-instantiating the CQCache
  }

  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: Id => fid(f)
      case f     => geocq.getReaderForFilter(f)
    }

  def fid(ids: Id): FR = {
    logger.debug("Queried for IDs; using Guava ID index")
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
  }
}


