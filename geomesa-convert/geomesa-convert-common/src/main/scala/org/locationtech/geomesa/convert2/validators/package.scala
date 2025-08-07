/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import io.micrometer.core.instrument.{Counter, Metrics, Tags}
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.convert2.metrics.ConverterMetrics
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry

import java.time.Instant
import java.util.Date

package object validators {

  /**
   * Create a counter with the standard validator prefix
   *
   * @param name counter name
   * @param tags tags
   * @return
   */
  def counter(name: String, tags: Tags): Counter = Metrics.counter(ConverterMetrics.name(s"validator.$name"), tags)

  /**
    * Validates an attribute is not null
    *
    * @param i attribute index
    * @param error error message
    * @param counter optional counter for validation failures
    */
  class NullValidator(i: Int, error: String, counter: Counter) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = {
      if (sf.getAttribute(i) != null) { null } else {
        counter.increment()
        error
      }
    }
    override def close(): Unit = {}
  }

  case object NoValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = null
    override def close(): Unit = {}
  }

  @deprecated("Replaced with IdValidatorFactory")
  case object IdValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String =
      if (sf.getID == null || sf.getID.isEmpty) { "feature ID is null" } else { null }
    override def close(): Unit = {}
  }

  object Errors {

    val GeomNull = "geometry is null"
    val DateNull = "date is null"

    private def format(d: Date): String = GeoToolsDateFormat.format(Instant.ofEpochMilli(d.getTime))

    def geomBounds(geom: Geometry): String =
      s"geometry exceeds world bounds ([-180,180][-90,90]): ${WKTUtils.write(geom)}"

    def dateBoundsLow(minDate: Date): Date => String = {
      val base = s"date is before minimum indexable date (${format(minDate)}): "
      date => base + format(date)
    }
    def dateBoundsHigh(maxDate: Date): Date => String = {
      val base = s"date is after maximum indexable date (${format(maxDate)}): "
      date => base + format(date)
    }
  }
}
