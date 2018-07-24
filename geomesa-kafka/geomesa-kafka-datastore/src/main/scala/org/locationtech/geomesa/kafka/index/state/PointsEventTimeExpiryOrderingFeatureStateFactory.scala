/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import java.util.concurrent.ScheduledExecutorService

import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory.{ExpiredState, FeatureExpiration, FeatureState, PointExpiryState}
import org.locationtech.geomesa.utils.cache.Ticker
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression

/**
  * Point feature state with event time ordering and timed expiration
  *
  * @param index spatial index
  * @param geom geometry attribute
  * @param executor executor
  * @param expiry expiry in millis
  * @param eventTime event time expression
  */
class PointsEventTimeExpiryOrderingFeatureStateFactory(index: SpatialIndex[SimpleFeature],
                                                       geom: Int,
                                                       eventTime: Expression,
                                                       expiration: FeatureExpiration,
                                                       executor: ScheduledExecutorService,
                                                       ticker: Ticker,
                                                       expiry: Long) extends FeatureStateFactory {

  override def createState(feature: SimpleFeature): FeatureState = {
    val time = FeatureStateFactory.time(eventTime, feature)
    val expiry = time + this.expiry - ticker.currentTimeMillis()
    if (expiry < 1L) {
      new ExpiredState(feature, time, expiration)
    } else {
      new PointExpiryState(feature, index, geom, time, expiration, executor, expiry)
    }
  }

  override def close(): Unit = executor.shutdownNow()
}
