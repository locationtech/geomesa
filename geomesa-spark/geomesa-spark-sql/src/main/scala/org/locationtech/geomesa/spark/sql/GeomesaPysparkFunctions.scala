/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.locationtech.geomesa.spark.jts.udf.GeometricAccessorFunctions._
import org.locationtech.geomesa.spark.jts.udf.GeometricCastFunctions._
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions._
import org.locationtech.geomesa.spark.jts.udf.GeometricOutputFunctions._
<<<<<<< HEAD
import org.locationtech.geomesa.spark.jts.udf.GeometricProcessingFunctions.{ST_BufferPoint, ST_MakeValid, ST_antimeridianSafeGeom}
=======
import org.locationtech.geomesa.spark.jts.udf.GeometricProcessingFunctions.{ST_BufferPoint, ST_antimeridianSafeGeom}
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._
import org.locationtech.geomesa.spark.sql.GeometricDistanceFunctions._

/**
 * Re-wrapping the UDFs so we can access them from PySpark without using the SQL API.
 */
object GeomesaPysparkFunctions {

  /* Geometric Accessor Functions */
  def st_boundary: UserDefinedFunction = udf(ST_Boundary)
  def st_coordDim: UserDefinedFunction = udf(ST_CoordDim)
  def st_dimension: UserDefinedFunction = udf(ST_Dimension)
  def st_envelope: UserDefinedFunction = udf(ST_Envelope)
  def st_exteriorRing: UserDefinedFunction = udf(ST_ExteriorRing)
  def st_geometryN: UserDefinedFunction = udf(ST_GeometryN)
  def st_interiorRingN: UserDefinedFunction = udf(ST_InteriorRingN)
  def st_isClosed: UserDefinedFunction = udf(ST_IsClosed)
  def st_isCollection: UserDefinedFunction = udf(ST_IsCollection)
  def st_isEmpty: UserDefinedFunction = udf(ST_IsEmpty)
  def st_isRing: UserDefinedFunction = udf(ST_IsRing)
  def st_isSimple: UserDefinedFunction = udf(ST_IsSimple)
  def st_isValid: UserDefinedFunction = udf(ST_IsValid)
  def st_numGeometries: UserDefinedFunction = udf(ST_NumGeometries)
  def st_numPoints: UserDefinedFunction = udf(ST_NumPoints)
  def st_pointN: UserDefinedFunction = udf(ST_PointN)
  def st_x: UserDefinedFunction = udf(ST_X)
  def st_y: UserDefinedFunction = udf(ST_Y)

  /* Geometric Cast Functions */
  def st_castToPoint: UserDefinedFunction = udf(ST_CastToPoint)
  def st_castToPolygon: UserDefinedFunction = udf(ST_CastToPolygon)
  def st_castToLineString: UserDefinedFunction = udf(ST_CastToLineString)
  def st_castToGeometry: UserDefinedFunction = udf(ST_CastToGeometry)
  def st_byteArray: UserDefinedFunction = udf(ST_ByteArray)

  /* Geometric Constructor Functions */
  def st_geomFromGeoHash: UserDefinedFunction = udf(ST_GeomFromGeoHash)
  def st_box2DFromGeoHash: UserDefinedFunction = udf(ST_GeomFromGeoHash)
  def st_geomFromGeoJSON: UserDefinedFunction = udf(ST_GeomFromGeoJSON)
  def st_geomFromText: UserDefinedFunction = udf(ST_GeomFromWKT)
  def st_geometryFromText: UserDefinedFunction = udf(ST_GeomFromWKT)
  def st_geomFromWKT: UserDefinedFunction = udf(ST_GeomFromWKT)
  def st_geomFromWKB: UserDefinedFunction = udf(ST_GeomFromWKB)
  def st_lineFromText: UserDefinedFunction = udf(ST_LineFromText)
  def st_makeBox2D: UserDefinedFunction = udf(ST_MakeBox2D)
  def st_makeBBOX: UserDefinedFunction = udf(ST_MakeBBOX)
  def st_makePolygon: UserDefinedFunction = udf(ST_MakePolygon)
  def st_makePoint: UserDefinedFunction = udf(ST_MakePoint)
  def st_makeLine: UserDefinedFunction = udf(ST_MakeLine)
  def st_makePointM: UserDefinedFunction = udf(ST_MakePointM)
  def st_mLineFromText: UserDefinedFunction = udf(ST_MLineFromText)
  def st_mPointFromText: UserDefinedFunction = udf(ST_MPointFromText)
  def st_mPolyFromText: UserDefinedFunction = udf(ST_MPolyFromText)
  def st_point: UserDefinedFunction = udf(ST_Point)
  def st_pointFromGeoHash: UserDefinedFunction = udf(ST_PointFromGeoHash)
  def st_pointFromText: UserDefinedFunction = udf(ST_PointFromText)
  def st_pointFromWKB: UserDefinedFunction = udf(ST_PointFromWKB)
  def st_polygon: UserDefinedFunction = udf(ST_Polygon)
  def st_polygonFromText: UserDefinedFunction = udf(ST_PolygonFromText)

  /* Geometric Output Functions */
  def st_asBinary: UserDefinedFunction = udf(ST_AsBinary)
  def st_asGeoJSON: UserDefinedFunction = udf(ST_AsGeoJSON)
  def st_asLatLonText: UserDefinedFunction = udf(ST_AsLatLonText)
  def st_asText: UserDefinedFunction = udf(ST_AsText)
  def st_geoHash: UserDefinedFunction = udf(ST_GeoHash)

  /* Geometric Processing Functions */
  def st_antimeridianSafeGeom: UserDefinedFunction = udf(ST_antimeridianSafeGeom)
  def st_bufferPoint: UserDefinedFunction = udf(ST_BufferPoint)
  def st_idlSafeGeom: UserDefinedFunction = udf(ST_antimeridianSafeGeom)
<<<<<<< HEAD
  def st_makeValid: UserDefinedFunction = udf(ST_MakeValid)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

  /* Spatial Relation Functions */
  def st_translate: UserDefinedFunction = udf(ST_Translate)
  def st_contains: UserDefinedFunction = udf(ST_Contains)
  def st_covers: UserDefinedFunction = udf(ST_Covers)
  def st_crosses: UserDefinedFunction = udf(ST_Crosses)
  def st_disjoint: UserDefinedFunction = udf(ST_Disjoint)
  def st_equals: UserDefinedFunction = udf(ST_Equals)
  def st_intersects: UserDefinedFunction = udf(ST_Intersects)
  def st_overlaps: UserDefinedFunction = udf(ST_Overlaps)
  def st_touches: UserDefinedFunction = udf(ST_Touches)
  def st_within: UserDefinedFunction = udf(ST_Within)
  def st_relate: UserDefinedFunction = udf(ST_Relate)
  def st_relateBool: UserDefinedFunction = udf(ST_RelateBool)
  def st_area: UserDefinedFunction = udf(ST_Area)
  def st_centroid: UserDefinedFunction = udf(ST_Centroid)
  def st_closestPoint: UserDefinedFunction = udf(ST_ClosestPoint)
  def st_distance: UserDefinedFunction = udf(ST_Distance)
  def st_distanceSphere: UserDefinedFunction = udf(ST_DistanceSphere)
  def st_distanceSpheroid: UserDefinedFunction = udf(ST_DistanceSpheroid)
  def st_length: UserDefinedFunction = udf(ST_Length)
  def st_lengthSphere: UserDefinedFunction = udf(ST_LengthSphere)
  def st_lengthSpheroid: UserDefinedFunction = udf(ST_LengthSpheroid)
  def st_intersection: UserDefinedFunction = udf(ST_Intersection)
  def st_difference: UserDefinedFunction = udf(ST_Difference)
  def st_transform: UserDefinedFunction = udf(ST_Transform)
}
