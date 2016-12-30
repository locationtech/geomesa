package org.apache.spark.sql

import com.vividsolutions.jts.geom._

object SQLSpatialAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = geom => geom.getBoundary
  val ST_CoordDim: Geometry => Int = geom => {
    val coord = geom.getCoordinate
    if (coord.z.isNaN) 2 else 3
  }
  val ST_Dimension: Geometry => Int = geom => geom.getDimension
  val ST_Envelope: Geometry => Geometry = geom => geom.getEnvelope
  val ST_ExteriorRing: Geometry => LineString = {
    case geom: Polygon => geom.getExteriorRing
    case _ => null
  }
  val ST_GeometryN: (Geometry, Int) => Geometry = (geom, n) => geom.getGeometryN(n-1)
  val ST_GeometryType: Geometry => String = geom => geom.getGeometryType
  val ST_InteriorRingN: (Geometry, Int) => Geometry = (geom, n) => {
    geom match {
      case geom: Polygon =>
        if (0 < n && n <= geom.getNumInteriorRing) {
          geom.getInteriorRingN(n-1)
        } else {
          null
        }
      case _ => null
    }
  }
  val ST_IsClosed: Geometry => Boolean = {
    case geom: LineString => geom.isClosed
    case geom: MultiLineString => geom.isClosed
    case _ => true
  }
  val ST_IsCollection: Geometry => Boolean = geom => geom.isInstanceOf[GeometryCollection]
  val ST_IsEmpty: Geometry => Boolean = geom => geom.isEmpty
  val ST_IsRing: Geometry => Boolean = {
    case geom: LineString => geom.isClosed && geom.isSimple
    case geom: MultiLineString => geom.isClosed && geom.isSimple
    case geom => geom.isSimple
  }
  val ST_IsSimple: Geometry => Boolean = geom => geom.isSimple
  val ST_IsValid: Geometry => Boolean = geom => geom.isValid
  val ST_NumGeometries: Geometry => Int = geom => geom.getNumGeometries
  val ST_NumPoints: Geometry => Int = geom => geom.getNumPoints
  val ST_PointN: (Geometry, Int) => Point = (geom, n) => {
    geom match {
      case geom: LineString =>
        if (n < 0) {
          geom.getPointN(n + geom.getLength.toInt)
        } else {
          geom.getPointN(n-1)
        }
      case _ => null
    }
  }
  val ST_X: Geometry => java.lang.Float = {
    case geom: Point => geom.getX.toFloat
    case _ => null
  }
  val ST_Y: Geometry => java.lang.Float = {
    case geom: Point => geom.getY.toFloat
    case _ => null
  }

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_boundary"      , ST_Boundary)
    sqlContext.udf.register("st_coordDim"      , ST_CoordDim)
    sqlContext.udf.register("st_dimension"     , ST_Dimension)
    sqlContext.udf.register("st_envelope"      , ST_Envelope)
    sqlContext.udf.register("st_exteriorRing"  , ST_ExteriorRing)
    sqlContext.udf.register("st_geometryN"     , ST_GeometryN)
    sqlContext.udf.register("st_geometryType"  , ST_GeometryType)
    sqlContext.udf.register("st_interiorRingN" , ST_InteriorRingN)
    sqlContext.udf.register("st_isClosed"      , ST_IsClosed)
    sqlContext.udf.register("st_isCollection"  , ST_IsCollection)
    sqlContext.udf.register("st_isEmpty"       , ST_IsEmpty)
    sqlContext.udf.register("st_isRing"        , ST_IsRing)
    sqlContext.udf.register("st_isSimple"      , ST_IsSimple)
    sqlContext.udf.register("st_isValid"       , ST_IsValid)
    sqlContext.udf.register("st_numGeometries" , ST_NumGeometries)
    sqlContext.udf.register("st_numPoints"     , ST_NumPoints)
    sqlContext.udf.register("st_pointN"        , ST_PointN)
    sqlContext.udf.register("st_x"             , ST_X)
    sqlContext.udf.register("st_y"             , ST_Y)
  }
}
