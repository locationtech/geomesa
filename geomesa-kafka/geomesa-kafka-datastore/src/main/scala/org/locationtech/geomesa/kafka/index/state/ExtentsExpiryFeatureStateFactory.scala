/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import java.util.concurrent.ScheduledExecutorService

import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory.{ExtentsExpiryState, FeatureExpiration, FeatureState}
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature

/**
  * Non-points feature state with timed expiration
  *
  * @param index spatial index
  * @param geom geometry attribute
  * @param executor executor
  * @param expiry expiry in millis
  */
class ExtentsExpiryFeatureStateFactory(index: SpatialIndex[SimpleFeature],
                                       geom: Int,
                                       expiration: FeatureExpiration,
                                       executor: ScheduledExecutorService,
                                       expiry: Long) extends FeatureStateFactory {

  override def createState(feature: SimpleFeature): FeatureState =
    new ExtentsExpiryState(feature, index, geom, 0L, expiration, executor, expiry)

  override def close(): Unit = executor.shutdownNow()
}
