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
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache.AbstractKafkaFeatureCache
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.concurrent.duration.Duration

class FeatureCacheCqEngine(sft: SimpleFeatureType,
                           expiry: Duration,
                           cleanup: Duration = Duration.Inf,
                           consistency: Duration = Duration.Inf)
                          (implicit ticker: Ticker)
    extends AbstractKafkaFeatureCache[SimpleFeature](expiry, cleanup, consistency) with LazyLogging {

  // TODO docs on cq indices

  private val cqEngine = new GeoCQEngine(sft)

  override def query(id: String): Option[SimpleFeature] = Option(cache.getIfPresent(id))

  override def query(filter: Filter): Iterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    filter match {
      case f: Id => f.getIDs.iterator.flatMap(id => Option(cache.getIfPresent(id.toString)).map(_.sf))
      case f     => cqEngine.getReaderForFilter(f)
    }
  }

  override protected def wrap(value: SimpleFeature): SimpleFeature = value

  override protected def addToIndex(value: SimpleFeature): Unit = cqEngine.add(value)

  override protected def removeFromIndex(value: SimpleFeature): Unit = cqEngine.remove(value)

  override protected def clearIndex(): Unit = cqEngine.clear() // TODO consider re-instantiating the cache instead

  override protected def inconsistencies(): Set[SimpleFeature] = {
    logger.warn("Inconsistency check not implemented for CQEngine cache")
    Set.empty
  }
}
