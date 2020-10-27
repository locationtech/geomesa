/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import org.joda.time.DateTime
import org.locationtech.geomesa.filter.{Bounds, FilterValues, filterToString}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}

import scala.concurrent.duration.{Duration, FiniteDuration}

package object guard {

  def filterString(filter: FilterStrategy[_, _, _]): String = filter.filter.map(filterToString).getOrElse("INCLUDE")

  def validate(intervals: FilterValues[Bounds[DateTime]], max: Duration): Boolean =
    intervals.nonEmpty && intervals.forall(_.isBoundedBothSides) && duration(intervals.values) <= max

  private def duration(values: Seq[Bounds[DateTime]]): FiniteDuration = {
    values.foldLeft(Duration.Zero) { (sum, bounds) =>
      sum + Duration(bounds.upper.value.get.toLocalDateTime.toDate.getTime - bounds.lower.value.get.toLocalDateTime.toDate.getTime, TimeUnit.MILLISECONDS)
    }
  }
}
