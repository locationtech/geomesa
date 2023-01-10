/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.sweepline.{SweepLineInterval, SweepLineOverlapAction}

import scala.collection.mutable.ListBuffer

class OverlapAction(
    leftIndex: Int,
    rightIndex: Int,
    conditionFunction: (Geometry, Geometry) => Boolean
  ) extends SweepLineOverlapAction with Serializable {

  val joinList: ListBuffer[(SimpleFeature, SimpleFeature)] = ListBuffer[(SimpleFeature, SimpleFeature)]()

  override def overlap(s0: SweepLineInterval, s1: SweepLineInterval): Unit = {
    val (key0, feature0) = s0.getItem.asInstanceOf[(Int, SimpleFeature)]
    val (key1, feature1) = s1.getItem.asInstanceOf[(Int, SimpleFeature)]
    if (key0 == 0 && key1 == 1) {
      val leftGeom = feature0.getAttribute(leftIndex).asInstanceOf[Geometry]
      val rightGeom = feature1.getAttribute(rightIndex).asInstanceOf[Geometry]
      if (conditionFunction(leftGeom, rightGeom)) {
        joinList.append((feature0, feature1))
      }
    } else if (key0 == 1 && key1 == 0) {
      val leftGeom = feature1.getAttribute(leftIndex).asInstanceOf[Geometry]
      val rightGeom = feature0.getAttribute(rightIndex).asInstanceOf[Geometry]
      if (conditionFunction(leftGeom, rightGeom)) {
        joinList.append((feature1, feature0))
      }
    }
  }
}
