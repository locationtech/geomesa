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
import org.locationtech.jts.util.GeometricShapeFactory
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Circle
import org.locationtech.spatial4j.shape.jts.JtsPoint

object GeometricProcessingFunctions extends UDFFactory {

  @transient private lazy val spatialContext = JtsSpatialContext.GEO
  @transient private lazy val shapeFactory   = spatialContext.getShapeFactory
  @transient private lazy val geometryFactory = new GeometryFactory()
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
    ST_AntimeridianSafeGeom(gsf.createCircle())
  }

  class ST_AntimeridianSafeGeom extends NullableUDF1[Geometry, Geometry](geom => {
    def degreesToTranslate(x: Double): Double = (((x + 180) / 360.0).floor * -360).toInt

    val geomCopy = geometryFactory.createGeometry(geom)
    if (geomCopy.getEnvelopeInternal.getMinX < -180 || geomCopy.getEnvelopeInternal.getMaxX > 180) {
      geomCopy.apply(new CoordinateSequenceFilter() {
        override def filter(seq: CoordinateSequence, i: Int): Unit = {
          seq.setOrdinate(i, CoordinateSequence.X, seq.getX(i) + degreesToTranslate(seq.getX(i)))
        }
        override def isDone: Boolean = false
        override def isGeometryChanged: Boolean = true
      })
    }

    val datelineSafeShape = shapeFactory.makeShapeFromGeometry(geomCopy)
    shapeFactory.getGeometryFrom(datelineSafeShape)
  })

  class ST_IdlSafeGeom extends ST_AntimeridianSafeGeom {
    override val name: String = "st_idlSafeGeom"
  }
  class ST_BufferPoint extends NullableUDF2[Point, Double, Geometry]((p, d) => {
    val degrees = DistanceUtils.dist2Degrees(d/1000.0, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    fastCircleToGeom(new JtsPoint(p, spatialContext).getBuffered(degrees, spatialContext))
  })

  val ST_AntimeridianSafeGeom = new ST_AntimeridianSafeGeom()
  val ST_IdlSafeGeom = new ST_IdlSafeGeom()
  val ST_BufferPoint = new ST_BufferPoint()

  override def udfs: Seq[Registerable] =
    Seq(
      ST_AntimeridianSafeGeom,
      ST_IdlSafeGeom,
      ST_BufferPoint
    )
}
