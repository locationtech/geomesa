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
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression

import scala.util.control.NonFatal

abstract class EventTimeFeatureCache(support: SpatialIndexSupport,
                                     geom: Int,
                                     expiry: Long,
                                     dtg: Expression,
                                     dtgOrdering: Boolean) extends BasicFeatureCache(support, geom, expiry) {

  override protected def time: Option[Expression] = Some(dtg)

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    val time = try {
      dtg.evaluate(feature) match {
        case d: Date   => d.getTime
        case d: Number => d.longValue()
        case d => Option(Converters.convert(d, classOf[Date])).map(_.getTime).getOrElse(0L)
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error evaluating event time for $feature", e); 0L
    }

    val featureState = createState(feature, time)
    if (expiry > 0) {
      putWithExpiry(featureState, expiry)
    } else {
      putWithoutExpiry(featureState)
    }

    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  private def putWithExpiry(featureState: FeatureState, expiry: Long): Unit = {
    val expiration = featureState.time + expiry - System.currentTimeMillis()
    if (expiration < 1) {
      logger.trace(s"${featureState.id} ignoring expired feature")
    } else {
      logger.trace(s"${featureState.id} adding feature with expiry $expiry")
      val old = state.put(featureState.id, featureState)
      if (old == null) {
        featureState.expire = executor.schedule(featureState, expiration, TimeUnit.MILLISECONDS)
        featureState.insertIntoIndex()
      } else if (!dtgOrdering || old.time <= featureState.time) {
        logger.trace(s"${featureState.id} removing old feature")
        old.expire.cancel(false)
        old.removeFromIndex()
        featureState.expire = executor.schedule(featureState, expiration, TimeUnit.MILLISECONDS)
        featureState.insertIntoIndex()
      } else {
        logger.trace(s"${featureState.id} ignoring out of sequence event time")
        if (!state.replace(featureState.id, featureState, old)) {
          logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
          old.removeFromIndex()
        }
      }
    }
  }

  private def putWithoutExpiry(featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} adding feature without expiry")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      featureState.insertIntoIndex()
    } else if (!dtgOrdering || old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      old.removeFromIndex()
      featureState.insertIntoIndex()
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence event time")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        old.removeFromIndex()
      }
    }
  }
}
