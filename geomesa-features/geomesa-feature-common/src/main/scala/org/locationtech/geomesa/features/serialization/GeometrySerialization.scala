/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.serialization

object GeometrySerialization {

  // 2-d values - corresponds to com.vividsolutions.jts.io.WKBConstants
  val Point2d: Int            = 1
  val LineString2d: Int       = 2
  val Polygon2d: Int          = 3

  val MultiPoint: Int         = 4
  val MultiLineString: Int    = 5
  val MultiPolygon: Int       = 6
  val GeometryCollection: Int = 7

  // n-dimensional values
  val Point: Int              = 8
  val LineString: Int         = 9
  val Polygon: Int            = 10
}