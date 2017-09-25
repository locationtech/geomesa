/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.github.benmanes.caffeine.cache._
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.index.FeatureCacheGuava.FeatureHolder
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache.AbstractKafkaFeatureCache
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

import scala.concurrent.duration.Duration

class FeatureCacheGuava(val sft: SimpleFeatureType, expiry: Duration, cleanup: Duration)(implicit ticker: Ticker)
    extends AbstractKafkaFeatureCache[FeatureHolder](expiry, cleanup)(ticker) with SpatialIndexSupport with LazyLogging {

  import scala.collection.JavaConverters._

  private val map = cache.asMap().asScala
  private var index = newSpatialIndex()

  override def spatialIndex: SpatialIndex[SimpleFeature] = index

  /**
    * WARNING: this method is not thread-safe
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def put(feature: SimpleFeature): Unit = {
    val id = feature.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      index.remove(old.env, old.sf)
    }
    val env = feature.geometry.getEnvelopeInternal
    index.insert(env, feature)
    cache.put(id, FeatureHolder(feature, env))
  }

  /**
    * WARNING: this method is not thread-safe
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def remove(id: String): Unit = {
    val old = cache.getIfPresent(id)
    if (old != null) {
      index.remove(old.env, old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    index = newSpatialIndex()
  }

  override def query(id: String): Option[SimpleFeature] = Option(cache.getIfPresent(id)).map(_.sf)

  override def query(filter: Filter): Iterator[SimpleFeature] = {
    filter match {
      case f: Id => f.getIDs.asScala.flatMap(id => Option(cache.getIfPresent(id.toString)).map(_.sf)).iterator
      case _ => super.query(filter)
    }
  }

  override def allFeatures(): Iterator[SimpleFeature] = map.valuesIterator.map(_.sf)

  override protected def expired(value: FeatureHolder): Unit = index.remove(value.env, value.sf)

  private def newSpatialIndex(): SpatialIndex[SimpleFeature] = new BucketIndex[SimpleFeature]
}

object FeatureCacheGuava {
  case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
    override def hashCode(): Int = sf.hashCode()
    override def equals(obj: scala.Any): Boolean = obj match {
      case other: FeatureHolder => sf.equals(other.sf)
      case _ => false
    }
  }
}