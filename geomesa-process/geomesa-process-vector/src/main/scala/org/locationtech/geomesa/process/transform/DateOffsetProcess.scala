/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.time._
import java.time.format.DateTimeParseException
import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.date.DateUtils.toInstant

import scala.collection.JavaConversions._

@DescribeProcess(
  title = "Date Offset Process",
  description = "Modifies the specified date field in a feature collection by an input time period."
)
class DateOffsetProcess extends GeoMesaProcess {

  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output collection")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              obsFeatures: SimpleFeatureCollection,
              @DescribeParameter(name = "dateField", description = "The date attribute to modify")
              dateField: String,
              @DescribeParameter(name = "timeOffset", description = "Time offset (e.g. P1D)")
              timeOffset: String): SimpleFeatureCollection = {

    val period = try { Duration.parse(timeOffset) } catch {
      case e: DateTimeParseException => throw new IllegalArgumentException(s"Invalid offset $timeOffset", e)
    }
    val dtgIndex = obsFeatures.getSchema.indexOf(dateField)
    require(dtgIndex != -1, s"Field '$dateField' does not exist in input feature collection: ${obsFeatures.getSchema}")

    val iter = SelfClosingIterator(obsFeatures.features()).map { sf =>
      val dtg = sf.getAttribute(dateField).asInstanceOf[Date]
      val offset = ZonedDateTime.ofInstant(toInstant(dtg), ZoneOffset.UTC).plus(period)
      val newDtg = Date.from(offset.toInstant)
      sf.setAttribute(dtgIndex, newDtg)
      sf
    }

    new ListFeatureCollection(obsFeatures.getSchema, iter.toList)
  }
}
