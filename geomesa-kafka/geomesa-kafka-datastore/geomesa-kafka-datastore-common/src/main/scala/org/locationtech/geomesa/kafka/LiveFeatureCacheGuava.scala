/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.{Executors, TimeUnit}

import com.github.benmanes.caffeine.cache._
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** @param sft the [[SimpleFeatureType]]
  * @param expirationPeriod the number of milliseconds after write to expire a feature or ``None`` to not
  *                         expire
  * @param ticker used to determine elapsed time for expiring entries
  */
class LiveFeatureCacheGuava(override val sft: SimpleFeatureType,
                            expirationPeriod: Option[Long],
                            consistencyCheck: Option[Long] = None)
                           (implicit ticker: Ticker)
  extends KafkaConsumerFeatureCache with LiveFeatureCache with LazyLogging {

  var spatialIndex: SpatialIndex[SimpleFeature] = newSpatialIndex()

  private val cache: Cache[String, FeatureHolder] = {
    val cb = Caffeine.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          override def onRemoval(key: String, value: FeatureHolder, cause: RemovalCause): Unit = {
            if (cause == RemovalCause.EXPIRED || cause == RemovalCause.EXPLICIT) {
              logger.debug(s"Removing feature $key due to expiration after ${ep}ms")
              spatialIndex.remove(value.env, value.sf)
            }
          }
        })
    }
    cb.build[String, FeatureHolder]()
  }

  private val consistencyChecker = consistencyCheck.map { interval =>
    import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

    val check = new Runnable() {

      private var lastRun = Set.empty[SimpleFeature]

      private def filter(feature: SimpleFeature, id: String): Boolean = feature.getID == id

      override def run(): Unit = {
        val cacheInconsistencies = features.collect {
          case (id, FeatureHolder(sf, env)) if spatialIndex.query(env, filter(_, id)).isEmpty => sf
        }.toSet
        val spatialInconsistencies = spatialIndex.query(wholeWorldEnvelope).filter { sf =>
          val cached = cache.getIfPresent(sf.getID)
          cached == null || cached.sf.geometry.getEnvelopeInternal != sf.geometry.getEnvelopeInternal
        }.toSet
        val union = cacheInconsistencies | spatialInconsistencies
        // only remove features that have been found to be inconsistent on the last run just to make sure
        lastRun = union.filter { sf =>
          if (lastRun.contains(sf)) {
            cache.invalidate(sf.getID)
            spatialIndex.remove(sf.geometry.getEnvelopeInternal, sf)
            false
          } else {
            true // we'll check it again next time
          }
        }
      }
    }
    val es = Executors.newSingleThreadScheduledExecutor()
    es.scheduleWithFixedDelay(check, interval, interval, TimeUnit.MILLISECONDS)
    es
  }

  override val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  override def cleanUp(): Unit = cache.cleanUp()

  /**
    * WARNING: this method is not thread-safe
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = sf.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
    }
    val env = sf.geometry.getEnvelopeInternal
    spatialIndex.insert(env, sf)
    cache.put(id, FeatureHolder(sf, env))
  }

  /**
    * WARNING: this method is not thread-safe
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    spatialIndex = newSpatialIndex()
  }

  override def getFeatureById(id: String): FeatureHolder = cache.getIfPresent(id)

  override def close(): Unit = consistencyChecker.foreach(_.shutdown())

  private def newSpatialIndex() = new BucketIndex[SimpleFeature]
}
