/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, Polygon}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.DataType
import org.locationtech.geomesa.utils.text.WKBUtils

object JoinHelperUtils {

  implicit val CoordinateOrdering: Ordering[Coordinate] =
    Ordering.by {
      case (c: Coordinate) => c.x
    }

  def unsafeRowToGeom(geometryOrdinal: Int, geometryType: DataType, row: UnsafeRow): Geometry = {
    val geometryData = row.get(geometryOrdinal, geometryType).asInstanceOf[UnsafeRow]
    val bytes: Array[Byte] = geometryData.getBinary(0)
    WKBUtils.read(bytes)
  }

  def rowOrdering(geometryOrdinal: Int, geometryType: DataType): Ordering[UnsafeRow] = {
    Ordering.by {
      case (row: UnsafeRow) =>
        val geom = unsafeRowToGeom(geometryOrdinal, geometryType, row)
        geom match {
          case (point: Point) => point.getCoordinate.x
          case (poly: Polygon) => poly.getCoordinates.min.x
        }
    }
  }
}
