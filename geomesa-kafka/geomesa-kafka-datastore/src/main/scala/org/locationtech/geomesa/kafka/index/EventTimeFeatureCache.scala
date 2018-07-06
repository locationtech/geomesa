/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.Date
import java.util.concurrent.TimeUnit

import org.geotools.util.Converters
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache.BasicFeatureCache
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression

import scala.util.control.NonFatal

class EventTimeFeatureCache(geom: Int, dtg: Expression, dtgOrdering: Boolean, expiry: Long, support: SpatialIndexSupport)
    extends BasicFeatureCache(geom, expiry, support) {

  override protected def time: Option[Expression] = Some(dtg)

  /**
    * WARNING: this method is not thread-safe
    *
    * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
    */
  override def put(feature: SimpleFeature): Unit = {
    val pt = geometry(feature)
    val time = try {
      dtg.evaluate(feature) match {
        case d: Date   => d.getTime
        case d: Number => d.longValue()
        case d => Option(Converters.convert(d, classOf[Date])).map(_.getTime).getOrElse(0L)
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error evaluating event time for $feature", e); 0L
    }

    val featureState = new FeatureState(pt.getX, pt.getY, feature.getID, time)
    if (expiry > 0) {
      putWithExpiry(feature, featureState, expiry)
    } else {
      putWithoutExpiry(feature, featureState)
    }

    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  private def putWithExpiry(feature: SimpleFeature, featureState: FeatureState, expiry: Long): Unit = {
    val expiration = featureState.time + expiry - System.currentTimeMillis()
    if (expiration < 1) {
      logger.trace(s"${featureState.id} ignoring expired feature")
    } else {
      logger.trace(s"${featureState.id} adding feature with expiry $expiry")
      val old = state.put(featureState.id, featureState)
      if (old == null) {
        featureState.expire = executor.schedule(featureState, expiration, TimeUnit.MILLISECONDS)
        support.index.insert(featureState.x, featureState.y, featureState.id, feature)
      } else if (!dtgOrdering || old.time <= featureState.time) {
        logger.trace(s"${featureState.id} removing old feature")
        old.expire.cancel(false)
        support.index.remove(old.x, old.y, old.id)
        featureState.expire = executor.schedule(featureState, expiration, TimeUnit.MILLISECONDS)
        support.index.insert(featureState.x, featureState.y, featureState.id, feature)
      } else {
        logger.trace(s"${featureState.id} ignoring out of sequence event time")
        if (!state.replace(featureState.id, featureState, old)) {
          logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
          support.index.remove(old.x, old.y, old.id)
        }
      }
    }
  }

  private def putWithoutExpiry(feature: SimpleFeature, featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} adding feature without expiry")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      support.index.insert(featureState.x, featureState.y, featureState.id, feature)
    } else if (!dtgOrdering || old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      support.index.remove(featureState.x, featureState.y, featureState.id)
      support.index.insert(featureState.x, featureState.y, featureState.id, feature)
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence event time")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        support.index.remove(old.x, old.y, old.id)
      }
    }
  }
}
