/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.{Column, TypedColumn}
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import java.{lang => jl}



/**
 * DataFrame spatial relations DSL
 */
object DataFrameFunctions extends SpatialEncoders {
  import org.locationtech.geomesa.spark.jts.encoders.SparkDefaultEncoders._

  trait SpatialConstructors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions._

    def st_geomFromWKT(wkt: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeomFromWKT, constructorNames, wkt)
    def st_geomFromWKT(wkt: String): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_GeomFromWKT, constructorNames, wkt)

    def st_geomFromWKB(wkb: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeomFromWKB, constructorNames, wkb)
    def st_geomFromWKB(wkb: Array[Byte]): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_GeomFromWKB, constructorNames, wkb)

    def st_lineFromText(wkt: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_LineFromText, constructorNames, wkt)
    def st_lineFromText(wkt: String): TypedColumn[Any, LineString] =
      udfToColumnLiterals(ST_LineFromText, constructorNames, wkt)

    def st_makeBox2D(lowerLeft: Column, upperRight: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_MakeBox2D, constructorNames, lowerLeft, upperRight)
    def st_makeBox2D(lowerLeft: Point, upperRight: Point): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_MakeBox2D, constructorNames, lowerLeft, upperRight)

    def st_makeBBOX(lowerX: Column, upperX: Column, lowerY: Column, upperY: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_MakeBBOX, constructorNames, lowerX, upperX, lowerY, upperY)
    def st_makeBBOX(lowerX: Double, upperX: Double, lowerY: Double, upperY: Double): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_MakeBBOX, constructorNames, lowerX, upperX, lowerY, upperY)

    def st_makePolygon(lineString: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_MakePolygon, constructorNames, lineString)
    def st_makePolygon(lineString: LineString): TypedColumn[Any, Polygon] =
      udfToColumnLiterals(ST_MakePolygon, constructorNames, lineString)

    def st_makePoint(x: Column, y: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_MakePoint, constructorNames, x, y)
    def st_makePoint(x: Double, y: Double): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_MakePoint, constructorNames, x, y)

    def st_makeLine(pointSeq: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_MakeLine, constructorNames, pointSeq)
    def st_makeLine(pointSeq: Seq[Point]): TypedColumn[Any, LineString] =
      udfToColumnLiterals(ST_MakeLine, constructorNames, pointSeq)

    def st_makePointM(x: Column, y: Column, m: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_MakePointM, constructorNames, x, y, m)
    def st_makePointM(x: Double, y: Double, m: Double): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_MakePointM, constructorNames, x, y, m)

    def st_mLineFromText(wkt: Column): TypedColumn[Any, MultiLineString] =
      udfToColumn(ST_MLineFromText, constructorNames, wkt)
    def st_mLineFromText(wkt: String): TypedColumn[Any, MultiLineString] =
      udfToColumnLiterals(ST_MLineFromText, constructorNames, wkt)

    def st_mPointFromText(wkt: Column): TypedColumn[Any, MultiPoint] =
      udfToColumn(ST_MPointFromText, constructorNames, wkt)
    def st_mPointFromText(wkt: String): TypedColumn[Any, MultiPoint] =
      udfToColumnLiterals(ST_MPointFromText, constructorNames, wkt)

    def st_mPolyFromText(wkt: Column): TypedColumn[Any, MultiPolygon] =
      udfToColumn(ST_MPolyFromText, constructorNames, wkt)
    def st_mPolyFromText(wkt: String): TypedColumn[Any, MultiPolygon] =
      udfToColumnLiterals(ST_MPolyFromText, constructorNames, wkt)

    def st_point(x: Column, y: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_Point, constructorNames, x, y)
    def st_point(x: Double, y: Double): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_Point, constructorNames, x, y)

    def st_pointFromText(wkt: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointFromText, constructorNames, wkt)
    def st_pointFromText(wkt: String): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_PointFromText, constructorNames, wkt)

    def st_pointFromWKB(wkb: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointFromWKB, constructorNames, wkb)
    def st_pointFromWKB(wkb: Array[Byte]): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_PointFromWKB, constructorNames, wkb)

    def st_polygon(lineString: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_Polygon, constructorNames, lineString)
    def st_polygon(lineString: LineString): TypedColumn[Any, Polygon] =
      udfToColumnLiterals(ST_Polygon, constructorNames, lineString)

    def st_polygonFromText(wkt: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_PolygonFromText, constructorNames, wkt)
    def st_polygonFromText(wkt: String): TypedColumn[Any, Polygon] =
      udfToColumnLiterals(ST_PolygonFromText, constructorNames, wkt)
  }

  trait SpatialCasters {
    import org.locationtech.geomesa.spark.jts.udf.GeometricCastFunctions._

    def st_castToPoint(geom: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_CastToPoint, castingNames, geom)
    def st_castToPoint(geom: Geometry): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_CastToPoint, castingNames, geom)

  
    def st_castToPolygon(geom: Column): TypedColumn[Any, Polygon] =
      udfToColumn(ST_CastToPolygon, castingNames, geom)
    def st_castToPolygon(geom: Geometry): TypedColumn[Any, Polygon] =
      udfToColumnLiterals(ST_CastToPolygon, castingNames, geom)

  
    def st_castToLineString(geom: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_CastToLineString, castingNames, geom)
    def st_castToLineString(geom: Geometry): TypedColumn[Any, LineString] =
      udfToColumnLiterals(ST_CastToLineString, castingNames, geom)

    def st_byteArray(str: Column): TypedColumn[Any, Array[Byte]] =
      udfToColumn(ST_ByteArray, castingNames, str)
    def st_byteArray(str: String): TypedColumn[Any, Array[Byte]] =
      udfToColumnLiterals(ST_ByteArray, castingNames, str)
  }

  trait SpatialAccessors {
    import org.locationtech.geomesa.spark.jts.udf.GeometricAccessorFunctions._

    def st_boundary(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Boundary, accessorNames, geom)
    def st_boundary(geom: Geometry): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_Boundary, accessorNames, geom)

    def st_coordDim(geom: Column): TypedColumn[Any, Int] =
      udfToColumn(ST_CoordDim, accessorNames, geom)
    def st_coordDim(geom: Geometry): TypedColumn[Any, Int] =
      udfToColumnLiterals(ST_CoordDim, accessorNames, geom)

    def st_dimension(geom: Column): TypedColumn[Any, Int] =
      udfToColumn(ST_Dimension, accessorNames, geom)
    def st_dimension(geom: Geometry): TypedColumn[Any, Int] =
      udfToColumnLiterals(ST_Dimension, accessorNames, geom)

    def st_envelope(geom: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Envelope, accessorNames, geom)
    def st_envelope(geom: Geometry): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_Envelope, accessorNames, geom)

    def st_exteriorRing(geom: Column): TypedColumn[Any, LineString] =
      udfToColumn(ST_ExteriorRing, accessorNames, geom)
    def st_exteriorRing(geom: Geometry): TypedColumn[Any, LineString] =
      udfToColumnLiterals(ST_ExteriorRing, accessorNames, geom)

    def st_geometryN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_GeometryN, accessorNames, geom, n)
    def st_geometryN(geom: Geometry, n: Int): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_GeometryN, accessorNames, geom, n)

    def st_geometryType(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_GeometryType, accessorNames, geom)
    def st_geometryType(geom: Geometry): TypedColumn[Any, String] =
      udfToColumnLiterals(ST_GeometryType, accessorNames, geom)

    def st_interiorRingN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_InteriorRingN, accessorNames, geom, n)
    def st_interiorRingN(geom: Geometry, n: Int): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_InteriorRingN, accessorNames, geom, n)

    def st_isClosed(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsClosed, accessorNames, geom)
    def st_isClosed(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsClosed, accessorNames, geom)

      def st_isCollection(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsCollection, accessorNames, geom)
    def st_isCollection(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsCollection, accessorNames, geom)

    def st_isEmpty(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsEmpty, accessorNames, geom)
    def st_isEmpty(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsEmpty, accessorNames, geom)

    def st_isRing(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsRing, accessorNames, geom)
    def st_isRing(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsRing, accessorNames, geom)

    def st_isSimple(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsSimple, accessorNames, geom)
    def st_isSimple(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsSimple, accessorNames, geom)

    def st_isValid(geom: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_IsValid, accessorNames, geom)
    def st_isValid(geom: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_IsValid, accessorNames, geom)

    def st_numGeometries(geom: Column): TypedColumn[Any, Int] =
      udfToColumn(ST_NumGeometries, accessorNames, geom)
    def st_numGeometries(geom: Geometry): TypedColumn[Any, Int] =
      udfToColumnLiterals(ST_NumGeometries, accessorNames, geom)

    def st_numPoints(geom: Column): TypedColumn[Any, Int] =
      udfToColumn(ST_NumPoints, accessorNames, geom)
    def st_numPoints(geom: Geometry): TypedColumn[Any, Int] =
      udfToColumnLiterals(ST_NumPoints, accessorNames, geom)

    def st_pointN(geom: Column, n: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_PointN, accessorNames, geom, n)
    def st_pointN(geom: Geometry, n: Int): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_PointN, accessorNames, geom, n)

    def st_x(geom: Column): TypedColumn[Any, jl.Float] =
      udfToColumn(ST_X, accessorNames, geom)
    def st_x(geom: Geometry): TypedColumn[Any, jl.Float] =
      udfToColumnLiterals(ST_X, accessorNames, geom)

    def st_y(geom: Column): TypedColumn[Any, jl.Float] =
      udfToColumn(ST_Y, accessorNames, geom)
    def st_y(geom: Geometry): TypedColumn[Any, jl.Float] =
      udfToColumnLiterals(ST_Y, accessorNames, geom)
  }

  trait SpatialOutputs {
    import org.locationtech.geomesa.spark.jts.udf.GeometricOutputFunctions._

    def st_asBinary(geom: Column): TypedColumn[Any, Array[Byte]] =
      udfToColumn(ST_AsBinary, outputNames, geom)
    def st_asBinary(geom: Geometry): TypedColumn[Any, Array[Byte]] =
      udfToColumnLiterals(ST_AsBinary, outputNames, geom)

    def st_asGeoJSON(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsGeoJSON, outputNames, geom)
    def st_asGeoJSON(geom: Geometry): TypedColumn[Any, String] =
      udfToColumnLiterals(ST_AsGeoJSON, outputNames, geom)

    def st_asLatLonText(point: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsLatLonText, outputNames, point)
    def st_asLatLonText(point: Point): TypedColumn[Any, String] =
      udfToColumnLiterals(ST_AsLatLonText, outputNames, point)

    def st_asText(geom: Column): TypedColumn[Any, String] =
      udfToColumn(ST_AsText, outputNames, geom)
    def st_asText(geom: Geometry): TypedColumn[Any, String] =
      udfToColumnLiterals(ST_AsText, outputNames, geom).as[String]

  }

  trait SpatialRelations {
    import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._

    def st_translate(geom: Column, deltaX: Column, deltaY: Column): TypedColumn[Any, Geometry] =
      udfToColumn(ST_Translate, relationNames, geom, deltaX, deltaY)
    def st_translate(geom: Geometry, deltaX: Double, deltaY: Double): TypedColumn[Any, Geometry] =
      udfToColumnLiterals(ST_Translate, relationNames, geom, deltaX, deltaY)

    def st_contains(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Contains, relationNames, left, right)
    def st_contains(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Contains, relationNames, left, right)

    def st_covers(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Covers, relationNames, left, right)
    def st_covers(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Covers, relationNames, left, right)

    def st_crosses(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Crosses, relationNames, left, right)
    def st_crosses(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Crosses, relationNames, left, right)

    def st_disjoint(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Disjoint, relationNames, left, right)
    def st_disjoint(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Disjoint, relationNames, left, right)

    def st_equals(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Equals, relationNames, left, right)
    def st_equals(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Equals, relationNames, left, right)

    def st_intersects(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Intersects, relationNames, left, right)
    def st_intersects(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Intersects, relationNames, left, right)

    def st_overlaps(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Overlaps, relationNames, left, right)
    def st_overlaps(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Overlaps, relationNames, left, right)

    def st_touches(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Touches, relationNames, left, right)
    def st_touches(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Touches, relationNames, left, right)

    def st_within(left: Column, right: Column): TypedColumn[Any, jl.Boolean] =
      udfToColumn(ST_Within, relationNames, left, right)
    def st_within(left: Geometry, right: Geometry): TypedColumn[Any, jl.Boolean] =
      udfToColumnLiterals(ST_Within, relationNames, left, right)

    def st_relate(left: Column, right: Column): TypedColumn[Any, String] =
      udfToColumn(ST_Relate, relationNames, left, right)
    def st_relate(left: Geometry, right: Geometry): TypedColumn[Any, String] =
      udfToColumnLiterals(ST_Relate, relationNames, left, right)

    def st_relateBool(left: Column, right: Column, pattern: Column): TypedColumn[Any, Boolean] =
      udfToColumn(ST_RelateBool, relationNames, left, right, pattern)
    def st_relateBool(left: Geometry, right: Geometry, pattern: String): TypedColumn[Any, Boolean] =
      udfToColumnLiterals(ST_RelateBool, relationNames, left, right, pattern)

    def st_area(geom: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Area, relationNames, geom)
    def st_area(geom: Geometry): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_Area, relationNames, geom)

    def st_closestPoint(left: Column, right: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_ClosestPoint, relationNames, left, right)
    def st_closestPoint(left: Geometry, right: Geometry): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_ClosestPoint, relationNames, left, right)

    def st_centroid(geom: Column): TypedColumn[Any, Point] =
      udfToColumn(ST_Centroid, relationNames, geom)
    def st_centroid(geom: Geometry): TypedColumn[Any, Point] =
      udfToColumnLiterals(ST_Centroid, relationNames, geom)

    def st_distance(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Distance, relationNames, left, right)
    def st_distance(left: Geometry, right: Geometry): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_Distance, relationNames, left, right)

    def st_distanceSpheroid(left: Column, right: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_DistanceSpheroid, relationNames, left, right)
    def st_distanceSpheroid(left: Geometry, right: Geometry): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_DistanceSpheroid, relationNames, left, right)

    def st_length(geom: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_Length, relationNames, geom)
    def st_length(geom: Geometry): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_Length, relationNames, geom)

    def st_aggregateDistanceSpheroid(geomSeq: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_AggregateDistanceSpheroid, relationNames, geomSeq)
    def st_aggregateDistanceSpheroid(geomSeq: Seq[Geometry]): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_AggregateDistanceSpheroid, relationNames, geomSeq)

    def st_lengthSpheroid(line: Column): TypedColumn[Any, jl.Double] =
      udfToColumn(ST_LengthSpheroid, relationNames, line)
    def st_lengthSpheroid(line: LineString): TypedColumn[Any, jl.Double] =
      udfToColumnLiterals(ST_LengthSpheroid, relationNames, line)
  }

  trait Library extends SpatialConstructors
    with SpatialCasters
    with SpatialAccessors
    with SpatialOutputs
    with SpatialRelations
}
