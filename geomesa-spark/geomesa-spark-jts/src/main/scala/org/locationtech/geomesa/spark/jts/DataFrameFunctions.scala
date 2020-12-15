/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import java.{lang => jl}

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{array, lit}
import org.apache.spark.sql.jts._
import org.apache.spark.sql.{Column, Encoder, Encoders, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.jts.geom._


/**
 * DataFrame DSL functions for working with JTS types
 */
object DataFrameFunctions extends SpatialEncoders {

  implicit def integerEncoder: Encoder[Integer] = Encoders.INT

  /**
   * Group of DataFrame DSL functions associated with constructing and encoding
   * JTS types in Spark SQL.
   */
  trait SpatialConstructors extends SpatialEncoders {
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
      ST_GeomFromGeoHash.toColumn(geohash, precision)
    def st_geomFromGeoHash(geohash: Column, precision: Int): TypedColumn[Any, Geometry] =
      st_geomFromGeoHash(geohash, lit(precision))

    def st_geomFromWKT(wkt: Column): TypedColumn[Any, Geometry] =
      ST_GeomFromWKT.toColumn(wkt)
    def st_geomFromWKT(wkt: String): TypedColumn[Any, Geometry] =
      st_geomFromWKT(lit(wkt))

    def st_geomFromWKB(wkb: Column): TypedColumn[Any, Geometry] =
      ST_GeomFromWKB.toColumn(wkb)
    def st_geomFromWKB(wkb: Array[Byte]): TypedColumn[Any, Geometry] =
      st_geomFromWKB(lit(wkb))

    def st_lineFromText(wkt: Column): TypedColumn[Any, LineString] =
      ST_LineFromText.toColumn(wkt)
    def st_lineFromText(wkt: String): TypedColumn[Any, LineString] =
      st_lineFromText(lit(wkt))

    def st_makeBox2D(lowerLeft: Column, upperRight: Column): TypedColumn[Any, Geometry] =
      ST_MakeBox2D.toColumn(lowerLeft, upperRight)
    def st_makeBox2D(lowerLeft: Point, upperRight: Point): TypedColumn[Any, Geometry] =
      st_makeBox2D(pointLit(lowerLeft), pointLit(upperRight))

    def st_makeBBOX(lowerX: Column, lowerY: Column, upperX: Column, upperY: Column): TypedColumn[Any, Geometry] =
      ST_MakeBBOX.toColumn(lowerX, lowerY, upperX, upperY)
    def st_makeBBOX(lowerX: Double, lowerY: Double, upperX: Double, upperY: Double): TypedColumn[Any, Geometry] =
      st_makeBBOX(lit(lowerX), lit(lowerY), lit(upperX), lit(upperY))

    def st_makePolygon(lineString: Column): TypedColumn[Any, Polygon] =
      ST_MakePolygon.toColumn(lineString)
    def st_makePolygon(lineString: LineString): TypedColumn[Any, Polygon] =
      st_makePolygon(lineLit(lineString))

    def st_makePoint(x: Column, y: Column): TypedColumn[Any, Point] =
      ST_MakePoint.toColumn(x, y)
    def st_makePoint(x: Double, y: Double): TypedColumn[Any, Point] =
      st_makePoint(lit(x), lit(y))

    def st_makeLine(pointSeq: Column): TypedColumn[Any, LineString] =
      ST_MakeLine.toColumn(pointSeq)
    def st_makeLine(pointSeq: Seq[Point]): TypedColumn[Any, LineString] =
      st_makeLine(array(pointSeq.map(pointLit): _*))

    def st_makePointM(x: Column, y: Column, m: Column): TypedColumn[Any, Point] =
      ST_MakePointM.toColumn(x, y, m)
    def st_makePointM(x: Double, y: Double, m: Double): TypedColumn[Any, Point] =
      st_makePointM(lit(x), lit(y), lit(m))

    def st_mLineFromText(wkt: Column): TypedColumn[Any, MultiLineString] =
      ST_MLineFromText.toColumn(wkt)
    def st_mLineFromText(wkt: String): TypedColumn[Any, MultiLineString] =
      st_mLineFromText(lit(wkt))

    def st_mPointFromText(wkt: Column): TypedColumn[Any, MultiPoint] =
      ST_MPointFromText.toColumn(wkt)
    def st_mPointFromText(wkt: String): TypedColumn[Any, MultiPoint] =
      st_mPointFromText(lit(wkt))

    def st_mPolyFromText(wkt: Column): TypedColumn[Any, MultiPolygon] =
      ST_MPolyFromText.toColumn(wkt)
    def st_mPolyFromText(wkt: String): TypedColumn[Any, MultiPolygon] =
      st_mPolyFromText(lit(wkt))

    def st_point(x: Column, y: Column): TypedColumn[Any, Point] =
      ST_Point.toColumn(x, y)
    def st_point(x: Double, y: Double): TypedColumn[Any, Point] =
      st_point(lit(x), lit(y))

    def st_pointFromGeoHash(geohash: Column, precision: Column): TypedColumn[Any, Point] =
      ST_PointFromGeoHash.toColumn(geohash, precision)
    def st_pointFromGeoHash(geohash: Column, precision: Int): TypedColumn[Any, Point] =
      st_pointFromGeoHash(geohash, lit(precision))

    def st_pointFromText(wkt: Column): TypedColumn[Any, Point] =
      ST_PointFromText.toColumn(wkt)
    def st_pointFromText(wkt: String): TypedColumn[Any, Point] =
      st_pointFromText(lit(wkt))

    def st_pointFromWKB(wkb: Column): TypedColumn[Any, Point] =
      ST_PointFromWKB.toColumn(wkb)
    def st_pointFromWKB(wkb: Array[Byte]): TypedColumn[Any, Point] =
      st_pointFromWKB(lit(wkb))

    def st_polygon(lineString: Column): TypedColumn[Any, Polygon] =
      ST_Polygon.toColumn(lineString)
    def st_polygon(lineString: LineString): TypedColumn[Any, Polygon] =
      st_polygon(lineLit(lineString))

    def st_polygonFromText(wkt: Column): TypedColumn[Any, Polygon] =
      ST_PolygonFromText.toColumn(wkt)
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
      ST_CastToPoint.toColumn(geom)

    def st_castToPolygon(geom: Column): TypedColumn[Any, Polygon] =
      ST_CastToPolygon.toColumn(geom)

    def st_castToLineString(geom: Column): TypedColumn[Any, LineString] =
      ST_CastToLineString.toColumn(geom)

    def st_castToGeometry(geom: Column): TypedColumn[Any, Geometry] =
      ST_CastToGeometry.toColumn(geom)

    def st_byteArray(str: Column): TypedColumn[Any, Array[Byte]] =
      ST_ByteArray.toColumn(str)
  }

  /**
   * Group of DataFrame DSL functions associated with fetching components
   * of JTS values.
   */
  trait SpatialAccessors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricAccessorFunctions._

    def st_boundary(geom: Column): TypedColumn[Any, Geometry] =
      ST_Boundary.toColumn(geom)

    def st_coordDim(geom: Column): TypedColumn[Any, Integer] =
      ST_CoordDim.toColumn(geom)

    def st_dimension(geom: Column): TypedColumn[Any, Integer] =
      ST_Dimension.toColumn(geom)

    def st_envelope(geom: Column): TypedColumn[Any, Geometry] =
      ST_Envelope.toColumn(geom)

    def st_exteriorRing(geom: Column): TypedColumn[Any, LineString] =
      ST_ExteriorRing.toColumn(geom)

    def st_geometryN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      ST_GeometryN.toColumn(geom, n)

    def st_geometryType(geom: Column): TypedColumn[Any, String] =
      ST_GeometryType.toColumn(geom)

    def st_interiorRingN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      ST_InteriorRingN.toColumn(geom, n)

    def st_isClosed(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsClosed.toColumn(geom)

      def st_isCollection(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsCollection.toColumn(geom)

    def st_isEmpty(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsEmpty.toColumn(geom)

    def st_isRing(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsRing.toColumn(geom)

    def st_isSimple(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsSimple.toColumn(geom)

    def st_isValid(geom: Column): TypedColumn[Any, jl.Boolean] =
      ST_IsValid.toColumn(geom)

    def st_numGeometries(geom: Column): TypedColumn[Any, Integer] =
      ST_NumGeometries.toColumn(geom)

    def st_numPoints(geom: Column): TypedColumn[Any, Integer] =
      ST_NumPoints.toColumn(geom)

    def st_pointN(geom: Column, n: Column): TypedColumn[Any, Point] =
      ST_PointN.toColumn(geom, n)

    def st_x(geom: Column): TypedColumn[Any, jl.Float] =
      ST_X.toColumn(geom)

    def st_y(geom: Column): TypedColumn[Any, jl.Float] =
      ST_Y.toColumn(geom)
  }

  /**
   * Group of DataFrame DSL functions associated with converting JTS
   * types into external formats.
   */
  trait SpatialOutputs {
    import org.locationtech.geomesa.spark.jts.udf.GeometricOutputFunctions._

    def st_asBinary(geom: Column): TypedColumn[Any, Array[Byte]] =
      ST_AsBinary.toColumn(geom)

    def st_asGeoJSON(geom: Column): TypedColumn[Any, String] =
      ST_AsGeoJSON.toColumn(geom)

    def st_asLatLonText(point: Column): TypedColumn[Any, String] =
      ST_AsLatLonText.toColumn(point)

    def st_asText(geom: Column): TypedColumn[Any, String] =
      ST_AsText.toColumn(geom)

    def st_geoHash(geom: Column, precision: Column): TypedColumn[Any, String] =
      ST_GeoHash.toColumn(geom, precision)
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
      ST_antimeridianSafeGeom.toColumn(geom)

    def st_bufferPoint(geom: Column, buffer: Column): TypedColumn[Any, Geometry] =
      ST_BufferPoint.toColumn(geom, buffer)
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
      ST_Translate.toColumn(geom, deltaX, deltaY)
    def st_translate(geom: Column, deltaX: Double, deltaY: Double): TypedColumn[Any, Geometry] =
      st_translate(geom, lit(deltaX), lit(deltaY))

    def st_contains(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Contains.toColumn(left, right)

    def st_covers(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Covers.toColumn(left, right)

    def st_crosses(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Crosses.toColumn(left, right)

    def st_disjoint(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Disjoint.toColumn(left, right)

    def st_equals(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Equals.toColumn(left, right)

    def st_intersects(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Intersects.toColumn(left, right)

    def st_overlaps(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Overlaps.toColumn(left, right)

    def st_touches(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Touches.toColumn(left, right)

    def st_within(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      ST_Within.toColumn(left, right)

    def st_relate(left: Column, right: Column): TypedColumn[Any, String] =
      ST_Relate.toColumn(left, right)

    def st_relateBool(left: Column, right: Column, pattern: Column): TypedColumn[Any, jl.Boolean] =
      ST_RelateBool.toColumn(left, right, pattern)

    def st_area(geom: Column): TypedColumn[Any, jl.Double] =
      ST_Area.toColumn(geom)

    def st_closestPoint(left: Column, right: Column): TypedColumn[Any, Point] =
      ST_ClosestPoint.toColumn(left, right)

    def st_centroid(geom: Column): TypedColumn[Any, Point] =
      ST_Centroid.toColumn(geom)

    def st_distance(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      ST_Distance.toColumn(left, right)

    def st_distanceSphere(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      ST_DistanceSphere.toColumn(left, right)

    def st_length(geom: Column): TypedColumn[Any, jl.Double] =
      ST_Length.toColumn(geom)

    def st_aggregateDistanceSphere(geomSeq: Column): TypedColumn[Any, jl.Double] =
      ST_AggregateDistanceSphere.toColumn(geomSeq)

    def st_lengthSphere(line: Column): TypedColumn[Any, jl.Double] =
      ST_LengthSphere.toColumn(line)

    def st_intersection(geom1:Column, geom2:Column): TypedColumn[Any, Geometry] =
      ST_Intersection.toColumn(geom1, geom2)

    def st_difference(geom1:Column, geom2:Column): TypedColumn[Any, Geometry] =
      ST_Difference.toColumn(geom1, geom2)
  }

  /** Stack of all DataFrame DSL functions. */
  trait Library extends SpatialConstructors
    with SpatialConverters
    with SpatialAccessors
    with SpatialOutputs
    with SpatialProcessors
    with SpatialRelations
}
