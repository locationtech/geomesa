/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.filters

import org.geotools.factory.CommonFactoryFinder
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.expression.Expression

object Filters {
  val ff = CommonFactoryFinder.getFilterFactory2

  def dt2lit(dt: DateTime): Expression = ff.literal(dt.toDate)

  def dts2lit(start: DateTime, end: DateTime): Expression = ff.literal(
    new DefaultPeriod(
      new DefaultInstant(new DefaultPosition(start.toDate)),
      new DefaultInstant(new DefaultPosition(end.toDate))
    ))

  def interval2lit(int: Interval): Expression = dts2lit(int.getStart, int.getEnd)
}
