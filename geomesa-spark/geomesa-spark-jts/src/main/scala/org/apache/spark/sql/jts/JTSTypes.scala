/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import com.vividsolutions.jts.geom._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
    classOf[Geometry]            -> classOf[GeometryUDT],
    classOf[Point]               -> classOf[PointUDT],
    classOf[LineString]          -> classOf[LineStringUDT],
    classOf[Polygon]             -> classOf[PolygonUDT],
    classOf[MultiPoint]          -> classOf[MultiPointUDT],
    classOf[MultiLineString]     -> classOf[MultiLineStringUDT],
    classOf[MultiPolygon]        -> classOf[MultiPolygonUDT],
    classOf[GeometryCollection]  -> classOf[GeometryCollectionUDT]
  )
}

private [spark] class PointUDT extends AbstractGeometryUDT[Point]("point")
object PointUDT extends PointUDT

private [spark] class MultiPointUDT extends AbstractGeometryUDT[MultiPoint]("multipoint")
object MultiPointUDT extends MultiPointUDT

private [spark] class LineStringUDT extends AbstractGeometryUDT[LineString]("linestring")
object LineStringUDT extends LineStringUDT

private [spark] class MultiLineStringUDT extends AbstractGeometryUDT[MultiLineString]("multilinestring")
object MultiLineStringUDT extends MultiLineStringUDT

private [spark] class PolygonUDT extends AbstractGeometryUDT[Polygon]("polygon")
object PolygonUDT extends PolygonUDT

private [spark] class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]("multipolygon")
object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class GeometryUDT extends AbstractGeometryUDT[Geometry]("geometry") {
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
  extends AbstractGeometryUDT[GeometryCollection]("geometrycollection")
object GeometryCollectionUDT extends GeometryCollectionUDT

object Test {
  import java.util.Calendar
  val cal = Calendar.getInstance()
  cal.set(Calendar.YEAR, cal.get(Calendar.YEAR) - 1)
  new java.sql.Timestamp(cal.getTimeInMillis)

  import java.text.DateFormat
  import java.text.SimpleDateFormat
  import java.util.TimeZone


  val tz: TimeZone = TimeZone.getTimeZone("UTC")
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'") // Quoted "Z" to indicate UTC, no timezone offset
  df.setTimeZone(tz)
  val nowAsISO: String = df.format()
}