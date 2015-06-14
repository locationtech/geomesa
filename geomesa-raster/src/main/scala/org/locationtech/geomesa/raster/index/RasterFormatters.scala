/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
