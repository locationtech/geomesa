/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.encoders

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

trait SpatialEncoders {
  implicit def jtsGeometryEncoder: Encoder[Geometry] = ExpressionEncoder()
  implicit def jtsPointEncoder: Encoder[Point] = ExpressionEncoder()
  implicit def jtsLineStringEncoder: Encoder[LineString] = ExpressionEncoder()
  implicit def jtsPolygonEncoder: Encoder[Polygon] = ExpressionEncoder()
  implicit def jtsMultiPointEncoder: Encoder[MultiPoint] = ExpressionEncoder()
  implicit def jtsMultiLineStringEncoder: Encoder[MultiLineString] = ExpressionEncoder()
  implicit def jtsMultiPolygonEncoder: Encoder[MultiPolygon] = ExpressionEncoder()
  implicit def jtsGeometryCollectionEncoder: Encoder[GeometryCollection] = ExpressionEncoder()
}

