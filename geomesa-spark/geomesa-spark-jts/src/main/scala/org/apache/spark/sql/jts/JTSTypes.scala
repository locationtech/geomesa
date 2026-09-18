/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.apache.spark.sql.jts

import org.apache.spark.sql.types.{DataType, UserDefinedType}

object JTSTypes {
  val GeometryTypeInstance           = new GeometryUDT
  val PointTypeInstance              = new PointUDT
  val LineStringTypeInstance         = new LineStringUDT
  val PolygonTypeInstance            = new PolygonUDT
  val MultiPointTypeInstance         = new MultiPointUDT
  val MultiLineStringTypeInstance    = new MultiLineStringUDT
  val MultipolygonTypeInstance       = new MultiPolygonUDT
  val GeometryCollectionTypeInstance = new GeometryCollectionUDT

  // these constant values conform to WKB values
  val GeometryType           = 0
  val PointType              = 1
  val LineStringType         = 2
  val PolygonType            = 3
  val MultiPointType         = 4
  val MultiLineStringType    = 5
  val MultiPolygonType       = 6
  val GeometryCollectionType = 7

  val typeMap: Map[Class[_], Class[_ <: UserDefinedType[_]]] = Map(
    classOf[org.locationtech.jts.geom.Geometry]            -> classOf[GeometryUDT],
    classOf[org.locationtech.jts.geom.Point]               -> classOf[PointUDT],
    classOf[org.locationtech.jts.geom.LineString]          -> classOf[LineStringUDT],
    classOf[org.locationtech.jts.geom.Polygon]             -> classOf[PolygonUDT],
    classOf[org.locationtech.jts.geom.MultiPoint]          -> classOf[MultiPointUDT],
    classOf[org.locationtech.jts.geom.MultiLineString]     -> classOf[MultiLineStringUDT],
    classOf[org.locationtech.jts.geom.MultiPolygon]        -> classOf[MultiPolygonUDT],
    classOf[org.locationtech.jts.geom.GeometryCollection]  -> classOf[GeometryCollectionUDT]
  )
}

private [spark] class PointUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.Point]("point")
object PointUDT extends PointUDT

private [spark] class MultiPointUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.MultiPoint]("multipoint")
object MultiPointUDT extends MultiPointUDT

private [spark] class LineStringUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.LineString]("linestring")
object LineStringUDT extends LineStringUDT

private [spark] class MultiLineStringUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.MultiLineString]("multilinestring")
object MultiLineStringUDT extends MultiLineStringUDT

private [spark] class PolygonUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.Polygon]("polygon")
object PolygonUDT extends PolygonUDT

private [spark] class MultiPolygonUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.MultiPolygon]("multipolygon")
object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class GeometryUDT extends AbstractGeometryUDT[org.locationtech.jts.geom.Geometry]("geometry") {
  private[sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
      dataType.getClass == JTSTypes.GeometryTypeInstance.getClass ||
      dataType.getClass == JTSTypes.PointTypeInstance.getClass ||
      dataType.getClass == JTSTypes.LineStringTypeInstance.getClass ||
      dataType.getClass == JTSTypes.PolygonTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultiLineStringTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultiPointTypeInstance.getClass ||
      dataType.getClass == JTSTypes.MultipolygonTypeInstance.getClass ||
      dataType.getClass == JTSTypes.GeometryCollectionTypeInstance.getClass
  }
}

case object GeometryUDT extends GeometryUDT

private [spark] class GeometryCollectionUDT
  extends AbstractGeometryUDT[org.locationtech.jts.geom.GeometryCollection]("geometrycollection")

object GeometryCollectionUDT extends GeometryCollectionUDT
