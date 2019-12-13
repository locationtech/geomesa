/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.encoders

import org.locationtech.jts.geom._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Encoders are Spark SQL's mechanism for converting a JVM type into a Catalyst representation.
 * They are fetched from implicit scope whenever types move beween RDDs and Datasets. Because each
 * of the types supported below has a corresponding UDT, we are able to use a standard Spark Encoder
 * to construct these implicits. */
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

