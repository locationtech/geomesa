/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.time.{Period, ZoneId}
import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

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

    val iter = CloseableIterator(obsFeatures.features())
    // TODO: Throw exception if dateField is not in SFT.
    val period = Period.parse(timeOffset)

    val iter2 = iter.map { sf =>

      val dtg = sf.getAttribute(dateField).asInstanceOf[Date]
      val ld = dtg.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
      val ld2 = ld.plus(period)
      val newDtg = Date.from(ld2.atStartOfDay(ZoneId.systemDefault()).toInstant)

      val updatedSF = ScalaSimpleFeature.copy(sf)
      updatedSF.setAttribute(dateField, newDtg)

      updatedSF
    }.toList

    new ListFeatureCollection(obsFeatures.getSchema, iter2)
  }
}