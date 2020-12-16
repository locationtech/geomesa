/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.jts.geom._

object GeometricAccessorFunctions extends UDFFactory {

  class ST_Boundary extends NullableUDF1[Geometry, Geometry](_.getBoundary)
  class ST_CoordDim extends NullableUDF1[Geometry, Integer](geom =>
    if (geom.getCoordinate.getZ.isNaN) 2 else 3
  )
  class ST_Dimension extends NullableUDF1[Geometry, Integer](_.getDimension)
  class ST_Envelope extends NullableUDF1[Geometry, Geometry](_.getEnvelope)
  class ST_ExteriorRing extends NullableUDF1[Geometry, LineString]({
    case geom: Polygon => geom.getExteriorRing
    case _ => null
  })
  class ST_GeometryN extends NullableUDF2[Geometry, Int, Geometry]((geom, n) =>
    if (n > 0 && n <= geom.getNumGeometries) { geom.getGeometryN(n - 1) } else { null }
  )
  class ST_GeometryType extends NullableUDF1[Geometry, String](_.getGeometryType)
  class ST_InteriorRingN extends NullableUDF2[Geometry, Int, Geometry]((geom, n) =>
    geom match {
      case geom: Polygon if 0 < n && n <= geom.getNumInteriorRing => geom.getInteriorRingN(n - 1)
      case _ => null
    }
  )
  class ST_IsClosed extends NullableUDF1[Geometry, java.lang.Boolean]({
    case geom: LineString => geom.isClosed
    case geom: MultiLineString => geom.isClosed
    case _ => true
  })
  class ST_IsCollection extends NullableUDF1[Geometry, java.lang.Boolean](_.isInstanceOf[GeometryCollection])
  class ST_IsEmpty extends NullableUDF1[Geometry, java.lang.Boolean](_.isEmpty)
  class ST_IsRing extends NullableUDF1[Geometry, java.lang.Boolean]({
    case geom: LineString => geom.isClosed && geom.isSimple
    case geom: MultiLineString => geom.isClosed && geom.isSimple
    case geom => geom.isSimple
  })
  class ST_IsSimple extends NullableUDF1[Geometry, java.lang.Boolean](_.isSimple)
  class ST_IsValid extends NullableUDF1[Geometry, java.lang.Boolean](_.isValid)
  class ST_NumGeometries extends NullableUDF1[Geometry, Integer](_.getNumGeometries)
  class ST_NumPoints extends NullableUDF1[Geometry, Integer](_.getNumPoints)
  class ST_PointN extends NullableUDF2[Geometry, Int, Point]((geom, n) =>
    geom match {
      case g: LineString if n > 0 && n <= g.getNumPoints => g.getPointN(n - 1)
      case g: LineString if n < 0 && n + g.getNumPoints >= 0 => g.getPointN(n + g.getNumPoints)
      case _ => null
    }
  )
  class ST_X extends NullableUDF1[Geometry, java.lang.Float]({
    case geom: Point => geom.getX.toFloat
    case _ => null
  })
  class ST_Y extends NullableUDF1[Geometry, java.lang.Float]({
    case geom: Point => geom.getY.toFloat
    case _ => null
  })

  val ST_Boundary = new ST_Boundary()
  val ST_CoordDim = new ST_CoordDim()
  val ST_Dimension = new ST_Dimension()
  val ST_Envelope = new ST_Envelope()
  val ST_ExteriorRing = new ST_ExteriorRing()
  val ST_GeometryN = new ST_GeometryN()
  val ST_GeometryType = new ST_GeometryType()
  val ST_InteriorRingN = new ST_InteriorRingN()
  val ST_IsClosed = new ST_IsClosed()
  val ST_IsCollection = new ST_IsCollection()
  val ST_IsEmpty = new ST_IsEmpty()
  val ST_IsRing = new ST_IsRing()
  val ST_IsSimple = new ST_IsSimple()
  val ST_IsValid = new ST_IsValid()
  val ST_NumGeometries = new ST_NumGeometries()
  val ST_NumPoints = new ST_NumPoints()
  val ST_PointN = new ST_PointN()
  val ST_X = new ST_X()
  val ST_Y = new ST_Y()

  override def udfs: Seq[Registerable] =
    Seq(
      ST_Boundary,
      ST_CoordDim,
      ST_Dimension,
      ST_Envelope,
      ST_ExteriorRing,
      ST_GeometryN,
      ST_GeometryType,
      ST_InteriorRingN,
      ST_IsClosed,
      ST_IsCollection,
      ST_IsEmpty,
      ST_IsRing,
      ST_IsSimple,
      ST_IsValid,
      ST_NumGeometries,
      ST_NumPoints,
      ST_PointN,
      ST_X,
      ST_Y
    )
}
