/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache._
import com.googlecode.cqengine.query.Query
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class LiveFeatureCacheCQEngine(sft: SimpleFeatureType,
                               expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends LiveFeatureCache with LazyLogging {

  val geocq = new GeoCQEngine(sft)

  // JNH: Do we need FeatureHolder anymore?
  private val cache: Cache[String, FeatureHolder] = {
    val cb = CacheBuilder.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            if (removal.getCause == RemovalCause.EXPIRED) {
              println(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")

              logger.debug(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")
              val ret = geocq.remove(removal.getValue.sf)
              println(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms returned $ret from CQEngine")
            }
          }
        })
    }
    cb.build()
  }

  val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  def size(): Int = {
    features.size
  }

  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      getReaderForFilter(f).getIterator.length
    }
  }

  override def cleanUp(): Unit = { cache.cleanUp() }

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

  override def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      //spatialIndex.remove(old.env, old.sf)
      geocq.remove(old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    geocq.clear()        // Consider re-instanting the CQCache
  }

  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: Id            => fid(f)
      case f                => geocq.getReaderForFilter(f)
      // JNH: Consider testing filter rewrite before passing to CQEngine?
    }

  def fid(ids: Id): FR = {
    println("Queried for IDs; using Guava ID index")
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
  }

  // TODO: Remove after testing is finished
  def getReaderForQuery(query: Query[SimpleFeature]): FR = {
    new DFR(sft, new DFI(geocq.getReaderForQuery(query).getIterator))
  }
}


