/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import com.vividsolutions.jts.geom._

trait SpatialEncoders {
  implicit def jtsGeometryEncoder = ExpressionEncoder[Geometry]()
  implicit def jtsPointEncoder = ExpressionEncoder[Point]()
  implicit def jtsLineStringEncoder = ExpressionEncoder[LineString]()
  implicit def jtsPolygonEncoder = ExpressionEncoder[Polygon]()
  implicit def jtsMultiPointEncoder = ExpressionEncoder[MultiPoint]()
  implicit def jtsMultiLineStringEncoder = ExpressionEncoder[MultiLineString]()
  implicit def jtsMultiPolygonEncoder = ExpressionEncoder[MultiPolygon]()
  implicit def jtsGeometryCollectionEncoder = ExpressionEncoder[GeometryCollection]()
}

object SpatialEncoders extends SpatialEncoders
