/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory.{ExtentsState, FeatureState}
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature

/**
  * Non-points feature state
  *
  * @param index spatial index
  * @param geom geometry attribute
  */
class ExtentsFeatureStateFactory(index: SpatialIndex[SimpleFeature], geom: Int) extends FeatureStateFactory {

  override def createState(feature: SimpleFeature): FeatureState = new ExtentsState(feature, index, geom, 0L)

  override def close(): Unit = {}
}
