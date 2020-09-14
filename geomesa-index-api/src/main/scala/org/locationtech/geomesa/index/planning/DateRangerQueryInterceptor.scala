/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.index.conf.QueryHints
import org.opengis.feature.simple.SimpleFeatureType
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.opengis.feature.simple.SimpleFeatureType

class DateRangerQueryInterceptor extends QueryInterceptor with LazyLogging {
  private var ds: DataStore = _
  private var sft: SimpleFeatureType = _
  /**
   * Called exactly once after the interceptor is instantiated
   *
   * @param ds  data store
   * @param sft simple feature type
   */
  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
    this.ds  = ds
    this.sft = sft
  }

  /**
   * Modifies the query in place
   *
   * @param query query
   */
  override def rewrite(query: Query): Unit = {
    QueryInterceptor.skipExecution.set(true)
    val plan = ds.asInstanceOf[GeoMesaDataStore[_]].getQueryPlan(query)
    QueryInterceptor.skipExecution.set(false)
    val index: GeoMesaFeatureIndex[_, _] = plan.head.filter.index

    index match {
      case _ : Z2Index | _ : XZ2Index |_ : Z3Index | _ : XZ3Index  =>
        val length = calculateDateRange(query)
        println(s"Length for query ${query.getFilter} is $length days")
        if (length >= 10) {
          throw new IllegalArgumentException("Date ranges need to be shorter than 10 days!")
        } else {
          logger.debug("Didn't update query")
        }
      case _ =>
        println("Got another index")
    }
  }

  private def calculateDateRange(query: Query): Long = {
    var length: Long = 0
    val temporalRange: FilterValues[Bounds[ZonedDateTime]] = FilterHelper.extractIntervals(query.getFilter, "dtg")

    logger.debug(s"Got ranges $temporalRange")
    temporalRange.foreach { bounds =>
      val upper: ZonedDateTime = bounds.upper.value.get
      val lower = bounds.lower.value.get
      length += Math.abs(ChronoUnit.DAYS.between(upper, lower))
    }
    logger.debug(s"Time range for dtg detected to be $length days.")
    length
  }

  override def close(): Unit = { }
}
