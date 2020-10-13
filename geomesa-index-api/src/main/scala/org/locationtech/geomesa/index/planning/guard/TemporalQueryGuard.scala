/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.index.TemporalIndexValues
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration
import scala.util.Try

class TemporalQueryGuard extends QueryInterceptor {

  import TemporalQueryGuard.Config
  import org.locationtech.geomesa.filter.filterToString

  private var max: Duration = _

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
    max = Try(Duration(sft.getUserData.get(Config).asInstanceOf[String])).getOrElse {
      throw new IllegalArgumentException(
        s"Temporal query guard expects valid duration under user data key '$Config'")
    }
  }

  override def rewrite(query: Query): Unit = {}

  override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = {
    val intervals = strategy.values.collect { case v: TemporalIndexValues => v.intervals }
    intervals.collect { case i if i.isEmpty || !i.forall(_.isBoundedBothSides) || duration(i.values) > max =>
      new IllegalArgumentException(
        s"Query exceeds maximum allowed filter duration of $max: ${filterToString(strategy.filter.filter)}")
    }
  }

  override def close(): Unit = {}

}

object TemporalQueryGuard {
  val Config = "geomesa.filter.max.duration"
}
