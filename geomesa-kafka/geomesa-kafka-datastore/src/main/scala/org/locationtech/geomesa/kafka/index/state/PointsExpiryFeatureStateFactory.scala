/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import java.util.concurrent.ScheduledExecutorService

import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory.{FeatureExpiration, FeatureState, PointExpiryState}
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature

/**
  * Point feature state with timed expiration
  *
  * @param index spatial index
  * @param geom geometry attribute index
  * @param executor executor service
  * @param expiry expiry in millis
  */
class PointsExpiryFeatureStateFactory(index: SpatialIndex[SimpleFeature],
                                      geom: Int,
                                      expiration: FeatureExpiration,
                                      executor: ScheduledExecutorService,
                                      expiry: Long) extends FeatureStateFactory {

  override def createState(feature: SimpleFeature): FeatureState =
    new PointExpiryState(feature, index, geom, 0L, expiration, executor, expiry)

  override def close(): Unit = executor.shutdownNow()
}
