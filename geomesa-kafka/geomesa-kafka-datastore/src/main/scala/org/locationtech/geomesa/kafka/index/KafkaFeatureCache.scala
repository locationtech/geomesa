/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.io.Closeable
import java.util.concurrent.{Executors, TimeUnit}

import com.github.benmanes.caffeine.cache._
import com.typesafe.scalalogging.LazyLogging
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration

trait KafkaFeatureCache extends Closeable {
  def put(feature: SimpleFeature): Unit
  def remove(id: String): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def query(id: String): Option[SimpleFeature]
  def query(filter: Filter): Iterator[SimpleFeature]
  def cleanUp(): Unit
}

object KafkaFeatureCache {

  def empty(): KafkaFeatureCache = EmptyFeatureCache

  abstract class AbstractKafkaFeatureCache[T <: AnyRef](expiry: Duration,
                                                        cleanup: Duration,
                                                        consistency: Duration)
                                                       (implicit ticker: Ticker)
      extends KafkaFeatureCache with LazyLogging {

    protected val cache: Cache[String, T] = {
      val builder = Caffeine.newBuilder().ticker(ticker)
      if (expiry != Duration.Inf) {
        val listener = new RemovalListener[String, T] {
          override def onRemoval(key: String, value: T, cause: RemovalCause): Unit = {
            if (cause != RemovalCause.REPLACED) {
              logger.debug(s"Removing feature $key due to ${cause.name()} after $expiry")
              removeFromIndex(value)
            }
          }
        }
        builder.expireAfterWrite(expiry.toMillis, TimeUnit.MILLISECONDS).removalListener(listener)
      }
      builder.build[String, T]()
    }

    private val executor = {
      val cleaner = if (expiry == Duration.Inf || cleanup == Duration.Inf) { None } else {
        Some(new Runnable() { override def run(): Unit = cache.cleanUp() })
      }
      val checker = if (consistency == Duration.Inf) { None } else {
        var lastRun = Set.empty[SimpleFeature]

        Some(new Runnable() {
          override def run(): Unit = {
            // only remove features that have been found to be inconsistent on the last run just to make sure
            lastRun = inconsistencies().filter { sf =>
              if (lastRun.contains(sf)) {
                cache.invalidate(sf.getID)
                removeFromIndex(wrap(sf))
                false
              } else {
                true // we'll check it again next time
              }
            }
          }
        })
      }

      val count = cleaner.map(_ => 1).getOrElse(0) + checker.map(_ => 1).getOrElse(0)
      if (count == 0) { None } else {
        val executor = Executors.newScheduledThreadPool(count)
        cleaner.foreach(executor.scheduleAtFixedRate(_, cleanup.toMillis, cleanup.toMillis, TimeUnit.MILLISECONDS))
        checker.foreach(executor.scheduleAtFixedRate(_, consistency.toMillis, consistency.toMillis, TimeUnit.MILLISECONDS))
        Some(executor)
      }
    }

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def put(feature: SimpleFeature): Unit = {
      val id = feature.getID
      val old = cache.getIfPresent(id)
      if (old != null) {
        removeFromIndex(old)
      }
      val wrapped = wrap(feature)
      cache.put(id, wrapped)
      addToIndex(wrapped)
    }

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def remove(id: String): Unit = {
      val old = cache.getIfPresent(id)
      if (old != null) {
        cache.invalidate(id)
        removeFromIndex(old)
      }
    }

    override def clear(): Unit = {
      cache.invalidateAll()
      clearIndex()
    }

    override def size(): Int = cache.estimatedSize().toInt

    // optimized for filter.include
    override def size(f: Filter): Int = {
      if (f == Filter.INCLUDE) { size() } else {
        query(f).length
      }
    }

    override def cleanUp(): Unit = cache.cleanUp()

    override def close(): Unit = executor.foreach(_.shutdown())

    /**
      * Create the cache value from a simple feature
      *
      * @param feature simple feature
      * @return
      */
    protected def wrap(feature: SimpleFeature): T

    /**
      * Add a feature to any indices
      *
      * @param value value to add
      */
    protected def addToIndex(value: T): Unit

    /**
      * Remove a feature from any indices
      *
      * @param value value to remove
      */
    protected def removeFromIndex(value: T): Unit

    /**
      * Clear all features from any indices
      */
    protected def clearIndex(): Unit

    /**
      * Check for inconsistencies between the main cache and the spatial index
      *
      * @return any inconsistent features (i.e. in one but not both indices)
      */
    protected def inconsistencies(): Set[SimpleFeature]
  }

  object EmptyFeatureCache extends KafkaFeatureCache {
    override def put(feature: SimpleFeature): Unit = throw new NotImplementedError("Empty feature cache")
    override def remove(id: String): Unit = throw new NotImplementedError("Empty feature cache")
    override def clear(): Unit = throw new NotImplementedError("Empty feature cache")
    override def size(): Int = 0
    override def size(filter: Filter): Int = 0
    override def query(id: String): Option[SimpleFeature] = None
    override def query(filter: Filter): Iterator[SimpleFeature] = Iterator.empty
    override def cleanUp(): Unit = {}
    override def close(): Unit = {}
  }
}