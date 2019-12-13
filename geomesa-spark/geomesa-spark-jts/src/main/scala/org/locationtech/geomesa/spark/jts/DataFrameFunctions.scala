/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import java.{lang => jl}

import org.locationtech.jts.geom._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{array, lit}
import org.apache.spark.sql.jts._
import org.apache.spark.sql.{Column, Encoder, Encoders, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._


/**
 * DataFrame DSL functions for working with JTS types
 */
object DataFrameFunctions extends SpatialEncoders {
  import org.locationtech.geomesa.spark.jts.encoders.SparkDefaultEncoders._

  implicit def integerEncoder: Encoder[Integer] = Encoders.INT

  /**
   * Group of DataFrame DSL functions associated with constructing and encoding
   * JTS types in Spark SQL.
   */
  trait SpatialConstructors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions._

    /** Constructs a geometric literal from a value  and JTS UDT */
    private def udtlit[T >: Null <: Geometry: Encoder, U <: AbstractGeometryUDT[T]](t: T, u: U): TypedColumn[Any, T] =
      new Column(Literal.create(u.serialize(t), u)).as[T]

    /** Create a generic geometry literal, encoded as a GeometryUDT. */
    def geomLit(g: Geometry): TypedColumn[Any, Geometry] = udtlit(g, GeometryUDT)
    /** Create a point literal, encoded as a PointUDT. */
    def pointLit(g: Point): TypedColumn[Any, Point] = udtlit(g, PointUDT)
    /** Create a line literal, encoded as a LineUDT. */
    def lineLit(g: LineString): TypedColumn[Any, LineString] = udtlit(g, LineStringUDT)
    /** Create a polygon literal, encoded as a PolygonUDT. */
    def polygonLit(g: Polygon): TypedColumn[Any, Polygon] = udtlit(g, PolygonUDT)
    /** Create a multi-point literal, encoded as a MultiPointUDT. */
    def mPointLit(g: MultiPoint): TypedColumn[Any, MultiPoint] = udtlit(g, MultiPointUDT)
    /** Create a multi-line literal, encoded as a MultiPointUDT. */
    def mLineLit(g: MultiLineString): TypedColumn[Any, MultiLineString] = udtlit(g, MultiLineStringUDT)
    /** Create a multi-polygon literal, encoded as a MultiPolygonUDT. */
    def mPolygonLit(g: MultiPolygon): TypedColumn[Any, MultiPolygon] = udtlit(g, MultiPolygonUDT)
    /** create a geometry collection literal, encoded as a GeometryCollectionUDT. */
    def geomCollLit(g: GeometryCollection): TypedColumn[Any, GeometryCollection] = udtlit(g, GeometryCollectionUDT)

    def st_geomFromGeoHash(geohash: Column, precision: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeomFromGeoHash, constructorNames, geohash, precision)
    def st_geomFromGeoHash(geohash: Column, precision: Int): TypedColumn[Any, Geometry] =
      st_geomFromGeoHash(geohash, lit(precision))

    def st_geomFromWKT(wkt: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeomFromWKT, constructorNames, wkt)
    def st_geomFromWKT(wkt: String): TypedColumn[Any, Geometry] =
      st_geomFromWKT(lit(wkt))

    def st_geomFromWKB(wkb: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeomFromWKB, constructorNames, wkb)
    def st_geomFromWKB(wkb: Array[Byte]): TypedColumn[Any, Geometry] =
      st_geomFromWKB(lit(wkb))

    def st_lineFromText(wkt: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_LineFromText, constructorNames, wkt)
    def st_lineFromText(wkt: String): TypedColumn[Any, LineString] =
      st_lineFromText(lit(wkt))

    def st_makeBox2D(lowerLeft: Column, upperRight: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_MakeBox2D, constructorNames, lowerLeft, upperRight)
    def st_makeBox2D(lowerLeft: Point, upperRight: Point): TypedColumn[Any, Geometry] =
      st_makeBox2D(pointLit(lowerLeft), pointLit(upperRight))

    def st_makeBBOX(lowerX: Column, lowerY: Column, upperX: Column, upperY: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_MakeBBOX, constructorNames, lowerX, lowerY, upperX, upperY)
    def st_makeBBOX(lowerX: Double, lowerY: Double, upperX: Double, upperY: Double): TypedColumn[Any, Geometry] =
      st_makeBBOX(lit(lowerX), lit(lowerY), lit(upperX), lit(upperY))

    def st_makePolygon(lineString: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_MakePolygon, constructorNames, lineString)
    def st_makePolygon(lineString: LineString): TypedColumn[Any, Polygon] =
      st_makePolygon(lineLit(lineString))

    def st_makePoint(x: Column, y: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_MakePoint, constructorNames, x, y)
    def st_makePoint(x: Double, y: Double): TypedColumn[Any, Point] =
      st_makePoint(lit(x), lit(y))

    def st_makeLine(pointSeq: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_MakeLine, constructorNames, pointSeq)
    def st_makeLine(pointSeq: Seq[Point]): TypedColumn[Any, LineString] =
      st_makeLine(array(pointSeq.map(pointLit): _*))

    def st_makePointM(x: Column, y: Column, m: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_MakePointM, constructorNames, x, y, m)
    def st_makePointM(x: Double, y: Double, m: Double): TypedColumn[Any, Point] =
      st_makePointM(lit(x), lit(y), lit(m))

    def st_mLineFromText(wkt: Column): TypedColumn[Any, MultiLineString] =
      udfToColumn(ST_MLineFromText, constructorNames, wkt)
    def st_mLineFromText(wkt: String): TypedColumn[Any, MultiLineString] =
      st_mLineFromText(lit(wkt))

    def st_mPointFromText(wkt: Column): TypedColumn[Any, MultiPoint] =
      udfToColumn(ST_MPointFromText, constructorNames, wkt)
    def st_mPointFromText(wkt: String): TypedColumn[Any, MultiPoint] =
      st_mPointFromText(lit(wkt))

    def st_mPolyFromText(wkt: Column): TypedColumn[Any, MultiPolygon] =
      udfToColumn(ST_MPolyFromText, constructorNames, wkt)
    def st_mPolyFromText(wkt: String): TypedColumn[Any, MultiPolygon] =
      st_mPolyFromText(lit(wkt))

    def st_point(x: Column, y: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_Point, constructorNames, x, y)
    def st_point(x: Double, y: Double): TypedColumn[Any, Point] =
      st_point(lit(x), lit(y))

    def st_pointFromGeoHash(geohash: Column, precision: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointFromGeoHash, constructorNames, geohash, precision)
    def st_pointFromGeoHash(geohash: Column, precision: Int): TypedColumn[Any, Point] =
      st_pointFromGeoHash(geohash, lit(precision))

    def st_pointFromText(wkt: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointFromText, constructorNames, wkt)
    def st_pointFromText(wkt: String): TypedColumn[Any, Point] =
      st_pointFromText(lit(wkt))

    def st_pointFromWKB(wkb: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointFromWKB, constructorNames, wkb)
    def st_pointFromWKB(wkb: Array[Byte]): TypedColumn[Any, Point] =
      st_pointFromWKB(lit(wkb))

    def st_polygon(lineString: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_Polygon, constructorNames, lineString)
    def st_polygon(lineString: LineString): TypedColumn[Any, Polygon] =
      st_polygon(lineLit(lineString))

    def st_polygonFromText(wkt: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_PolygonFromText, constructorNames, wkt)
    def st_polygonFromText(wkt: String): TypedColumn[Any, Polygon] =
      st_polygonFromText(lit(wkt))
  }

  /**
   * Group of DataFrame DSL functions associated with converting JTS types
   * from one type to another.
   */
  trait SpatialConverters {
    import org.locationtech.geomesa.spark.jts.udf.GeometricCastFunctions._

    def st_castToPoint(geom: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_CastToPoint, castingNames, geom)

    def st_castToPolygon(geom: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_CastToPolygon, castingNames, geom)

    def st_castToLineString(geom: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_CastToLineString, castingNames, geom)

    def st_castToGeometry(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_CastToGeometry, castingNames, geom)

    def st_byteArray(str: Column): TypedColumn[Any, Array[Byte]] =
      udfToColumn(ST_ByteArray, castingNames, str)
  }

  /**
   * Group of DataFrame DSL functions associated with fetching components
   * of JTS values.
   */
  trait SpatialAccessors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricAccessorFunctions._

    def st_boundary(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Boundary, accessorNames, geom)

    def st_coordDim(geom: Column): TypedColumn[Any, Integer] =
      udfToColumn(ST_CoordDim, accessorNames, geom)

    def st_dimension(geom: Column): TypedColumn[Any, Integer] =
      udfToColumn(ST_Dimension, accessorNames, geom)

    def st_envelope(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Envelope, accessorNames, geom)

    def st_exteriorRing(geom: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_ExteriorRing, accessorNames, geom)

    def st_geometryN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeometryN, accessorNames, geom, n)

    def st_geometryType(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_GeometryType, accessorNames, geom)

    def st_interiorRingN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_InteriorRingN, accessorNames, geom, n)

    def st_isClosed(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsClosed, accessorNames, geom)

      def st_isCollection(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsCollection, accessorNames, geom)

    def st_isEmpty(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsEmpty, accessorNames, geom)

    def st_isRing(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsRing, accessorNames, geom)

    def st_isSimple(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsSimple, accessorNames, geom)

    def st_isValid(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsValid, accessorNames, geom)

    def st_numGeometries(geom: Column): TypedColumn[Any, Integer] =
      udfToColumn(ST_NumGeometries, accessorNames, geom)

    def st_numPoints(geom: Column): TypedColumn[Any, Integer] =
      udfToColumn(ST_NumPoints, accessorNames, geom)

    def st_pointN(geom: Column, n: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointN, accessorNames, geom, n)

    def st_x(geom: Column): TypedColumn[Any, jl.Float] =
      udfToColumn(ST_X, accessorNames, geom)

    def st_y(geom: Column): TypedColumn[Any, jl.Float] =
      udfToColumn(ST_Y, accessorNames, geom)
  }

  /**
   * Group of DataFrame DSL functions associated with converting JTS
   * types into external formats.
   */
  trait SpatialOutputs {
    import org.locationtech.geomesa.spark.jts.udf.GeometricOutputFunctions._

    def st_asBinary(geom: Column): TypedColumn[Any, Array[Byte]] =
      udfToColumn(ST_AsBinary, outputNames, geom)

    def st_asGeoJSON(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsGeoJSON, outputNames, geom)

    def st_asLatLonText(point: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsLatLonText, outputNames, point)

    def st_asText(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsText, outputNames, geom)

    def st_geoHash(geom: Column, precision: Column): TypedColumn[Any, String] =
      udfToColumn(ST_GeoHash, outputNames, geom, precision)
    def st_geoHash(geom: Column, precision: Int): TypedColumn[Any, String] =
      st_geoHash(geom, lit(precision))
  }

  /**
    * Group of DataFrame DSL functions associated with processing
    * and manipulating JTS Types
    */
  trait SpatialProcessors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricProcessingFunctions._

    def st_antimeridianSafeGeom(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_antimeridianSafeGeom, processingNames, geom)

    def st_bufferPoint(geom: Column, buffer: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_BufferPoint, processingNames, geom, buffer)
    def st_bufferPoint(geom: Column, buffer: Double): TypedColumn[Any, Geometry] =
      st_bufferPoint(geom, lit(buffer))
  }

  /**
   * Group of DataFrame DSL functions associated with determining the relationship
   * between two JTS values.
   */
  trait SpatialRelations {
    import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._

    def st_translate(geom: Column, deltaX: Column, deltaY: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Translate, relationNames, geom, deltaX, deltaY)
    def st_translate(geom: Column, deltaX: Double, deltaY: Double): TypedColumn[Any, Geometry] =
      st_translate(geom, lit(deltaX), lit(deltaY))

    def st_contains(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Contains, relationNames, left, right)

    def st_covers(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Covers, relationNames, left, right)

    def st_crosses(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Crosses, relationNames, left, right)

    def st_disjoint(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Disjoint, relationNames, left, right)

    def st_equals(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Equals, relationNames, left, right)

    def st_intersects(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Intersects, relationNames, left, right)

    def st_overlaps(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Overlaps, relationNames, left, right)

    def st_touches(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Touches, relationNames, left, right)

    def st_within(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Within, relationNames, left, right)

    def st_relate(left: Column, right: Column): TypedColumn[Any, String] =
      udfToColumn(ST_Relate, relationNames, left, right)

    def st_relateBool(left: Column, right: Column, pattern: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_RelateBool, relationNames, left, right, pattern)

    def st_area(geom: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Area, relationNames, geom)

    def st_closestPoint(left: Column, right: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_ClosestPoint, relationNames, left, right)

    def st_centroid(geom: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_Centroid, relationNames, geom)

    def st_distance(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Distance, relationNames, left, right)

    def st_distanceSphere(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_DistanceSphere, relationNames, left, right)

    def st_length(geom: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Length, relationNames, geom)

    def st_aggregateDistanceSphere(geomSeq: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_AggregateDistanceSphere, relationNames, geomSeq)

    def st_lengthSphere(line: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_LengthSphere, relationNames, line)
  }

  /** Stack of all DataFrame DSL functions. */
  trait Library extends SpatialConstructors
    with SpatialConverters
    with SpatialAccessors
    with SpatialOutputs
    with SpatialProcessors
    with SpatialRelations
}
