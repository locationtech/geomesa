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

class FeatureCacheGuava(val sft: SimpleFeatureType,
                        expiry: Duration,
                        cleanup: Duration = Duration.Inf,
                        consistency: Duration = Duration.Inf)
                       (implicit ticker: Ticker)
    extends AbstractKafkaFeatureCache[FeatureHolder](expiry, cleanup, consistency)(ticker)
      with SpatialIndexSupport with LazyLogging {

  import scala.collection.JavaConverters._

  private val map = cache.asMap().asScala
  private var index = newSpatialIndex()

  override def spatialIndex: SpatialIndex[SimpleFeature] = index

  override def query(id: String): Option[SimpleFeature] = Option(cache.getIfPresent(id)).map(_.sf)

  override def query(filter: Filter): Iterator[SimpleFeature] = {
    filter match {
      case f: Id => f.getIDs.asScala.flatMap(id => Option(cache.getIfPresent(id.toString)).map(_.sf)).iterator
      case _ => super.query(filter)
    }
  }

  override def allFeatures(): Iterator[SimpleFeature] = map.valuesIterator.map(_.sf)

  override protected def wrap(value: SimpleFeature): FeatureHolder =
    FeatureHolder(value, value.geometry.getEnvelopeInternal)

  override protected def addToIndex(value: FeatureHolder): Unit = index.insert(value.env, value.sf)

  override protected def removeFromIndex(value: FeatureHolder): Unit = index.remove(value.env, value.sf)

  override protected def clearIndex(): Unit = index = newSpatialIndex()

  override protected def inconsistencies(): Set[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

    val cacheInconsistencies = map.iterator.collect {
      case (id, holder) if index.query(holder.env, (f) => f.getID == id).isEmpty => holder.sf
    }
    val spatialInconsistencies = index.query(wholeWorldEnvelope).filter { sf =>
      val cached = cache.getIfPresent(sf.getID)
      cached == null || cached.sf.geometry.getEnvelopeInternal != sf.geometry.getEnvelopeInternal
    }

    cacheInconsistencies.toSet | spatialInconsistencies.toSet
  }

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