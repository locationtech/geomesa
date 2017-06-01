/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.geotools.factory.CommonFactoryFinder
import org.joda.time.Interval
import org.locationtech.geomesa.utils.time.Time._
import org.opengis.feature.{Feature, FeatureVisitor}

class MinMaxTimeVisitor(dtg: String) extends FeatureVisitor {
  val factory = CommonFactoryFinder.getFilterFactory(null)
  val expr = factory.property(dtg)

  private var timeBounds: Interval = null

  // TODO?: Convert to CalcResults/etc?
  def getBounds = timeBounds

  def updateBounds(date: Date): Unit = {
    timeBounds = timeBounds.expandByDate(date)
  }

  override def visit(p1: Feature): Unit = {
    val date = expr.evaluate(p1).asInstanceOf[Date]
    if (date != null) {
      updateBounds(date)
    }
  }
}
