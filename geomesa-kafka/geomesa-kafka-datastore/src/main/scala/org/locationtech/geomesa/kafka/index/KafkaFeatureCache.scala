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

  abstract class AbstractKafkaFeatureCache[T <: AnyRef](expiry: Duration, cleanup: Duration)(implicit ticker: Ticker)
      extends KafkaFeatureCache with LazyLogging {

    protected val cache: Cache[String, T] = {
      val builder = Caffeine.newBuilder().ticker(ticker)
      if (expiry != Duration.Inf) {
        val listener = new RemovalListener[String, T] {
          override def onRemoval(key: String, value: T, cause: RemovalCause): Unit = {
            if (cause == RemovalCause.EXPIRED) {
              logger.debug(s"Removing feature $key due to expiration after $expiry")
              expired(value)
            }
          }
        }
        builder.expireAfterWrite(expiry.toMillis, TimeUnit.MILLISECONDS).removalListener(listener)
      }
      builder.build[String, T]()
    }

    private val cleaner = if (expiry == Duration.Inf || cleanup == Duration.Inf) { None } else {
      val executor = Executors.newSingleThreadScheduledExecutor()
      val runnable = new Runnable() { override def run(): Unit = cache.cleanUp() }
      executor.scheduleAtFixedRate(runnable, cleanup.toMillis, cleanup.toMillis, TimeUnit.MILLISECONDS)
      Some(executor)
    }

    protected def expired(value: T): Unit

    override def size(): Int = cache.estimatedSize().toInt

    // optimized for filter.include
    override def size(f: Filter): Int = {
      if (f == Filter.INCLUDE) { size() } else {
        query(f).length
      }
    }

    override def cleanUp(): Unit = cache.cleanUp()

    override def close(): Unit = cleaner.foreach(_.shutdown())
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