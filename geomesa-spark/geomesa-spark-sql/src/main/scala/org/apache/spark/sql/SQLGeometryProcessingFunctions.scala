/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Coordinate, Point, Polygon}
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.apache.spark.sql.SQLFunctionHelper.nullableUDF
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Circle
import org.locationtech.spatial4j.shape.jts.JtsPoint

import scala.util.{Failure, Success}

object SQLGeometryProcessingFunctions extends LazyLogging {

  @transient private lazy val spatialContext = JtsSpatialContext.GEO
  @transient private lazy val shapeFactory   = spatialContext.getShapeFactory
  @transient private lazy val geometryFactory = JTSFactoryFinder.getGeometryFactory
  @transient private val geometricShapeFactory =
    new ThreadLocal[GeometricShapeFactory] {
      override def initialValue(): GeometricShapeFactory = {
        new GeometricShapeFactory(geometryFactory)
      }
    }

  private def fastCircleToGeom(circle: Circle): Geometry = {
    val gsf = geometricShapeFactory.get()
    gsf.setSize(circle.getBoundingBox.getWidth)
    gsf.setNumPoints(4*25) //multiple of 4 is best
    gsf.setCentre(new Coordinate(circle.getCenter.getX, circle.getCenter.getY))
    ST_antimeridianSafeGeom(gsf.createCircle())
  }

  val ST_antimeridianSafeGeom: Geometry => Geometry = nullableUDF(geom => {
    GeohashUtils.getInternationalDateLineSafeGeometry(geom) match {
      case Success(g) => g
      case Failure(e) =>
        logger.warn(s"Error splitting geometry on anti-meridian for $geom", e)
        geom
    }
  })

  val ST_BufferPoint: (Point, Double) => Geometry = (p, d) => {
    val degrees = DistanceUtils.dist2Degrees(d/1000.0, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    fastCircleToGeom(new JtsPoint(p, spatialContext).getBuffered(degrees, spatialContext))
  }

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_antimeridianSafeGeom", ST_antimeridianSafeGeom)
    sqlContext.udf.register("st_bufferPoint"         , ST_BufferPoint)
    sqlContext.udf.register("st_idlSafeGeom"         , ST_antimeridianSafeGeom)
  }

}
