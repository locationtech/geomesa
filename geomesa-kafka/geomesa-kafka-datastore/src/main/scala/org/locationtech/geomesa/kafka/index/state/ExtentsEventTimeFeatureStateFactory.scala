/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory._
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression

/**
  * Non-point feature state with even time ordering
  *
  * @param index spatial index
  * @param geom geometry attribute
  * @param eventTime event time expression
  */
class ExtentsEventTimeFeatureStateFactory(index: SpatialIndex[SimpleFeature], geom: Int, eventTime: Expression)
    extends FeatureStateFactory with LazyLogging {

  override def createState(feature: SimpleFeature): FeatureState =
    new ExtentsState(feature, index, geom, FeatureStateFactory.time(eventTime, feature))

  override def close(): Unit = {}
}
