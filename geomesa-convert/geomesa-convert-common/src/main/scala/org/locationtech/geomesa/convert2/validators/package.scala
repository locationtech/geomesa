/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.time.Instant
import java.util.Date

import com.codahale.metrics.Counter
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry

import java.time.Instant
import java.util.Date

package object validators {

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
        counter.inc()
        error
      }
    }
    override def close(): Unit = {}
  }

  case object NoValidator extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): String = null
    override def close(): Unit = {}
  }

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
