/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.raster.index

import org.apache.hadoop.io.Text
import org.joda.time.DateTime
import org.locationtech.geomesa.accumulo.index.TextFormatter
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.opengis.feature.simple.SimpleFeature

/**
 * Responsible for representing the resolution by encoding a double lexicographically.
 *
 * @param number
 */
case class DoubleTextFormatter(number: Double) extends TextFormatter {
  val fmtdStr: String = lexiEncodeDoubleToString(number)
  val numBits: Int = fmtdStr.length
  def formatString(gh: GeoHash, dt: DateTime, sf: SimpleFeature) = fmtdStr

  override def format(gh: GeoHash, dt: DateTime, sf: SimpleFeature, isIndex: Boolean): Text = new Text(fmtdStr)
}
