package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import com.vividsolutions.jts.util.GeometricShapeFactory
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.spatial4j.context.jts.JtsSpatialContext
import org.locationtech.spatial4j.distance.DistanceUtils
import org.locationtech.spatial4j.shape.Circle
import org.locationtech.spatial4j.shape.jts.JtsPoint

object SQLGeometryProcessingFunctions {

  @transient private lazy val spatialContext = JtsSpatialContext.GEO
  @transient private lazy val shapeFactory   = spatialContext.getShapeFactory
  @transient private lazy val geometryFactory = JTSFactoryFinder.getGeometryFactory
  @transient private val geometricShapeFactory =
    new ThreadLocal[GeometricShapeFactory] {
      override def initialValue(): GeometricShapeFactory = {
        new GeometricShapeFactory(geometryFactory)
      }
    }

  private def fastCircleToPoly(circle: Circle) = {
    if (circle.getBoundingBox.getCrossesDateLine)
      throw new IllegalArgumentException("Doesn't support dateline cross yet: "+circle)

    val gsf = geometricShapeFactory.get()
    gsf.setSize(circle.getBoundingBox.getWidth)
    gsf.setNumPoints(4*25) //multiple of 4 is best
    gsf.setCentre(new Coordinate(circle.getCenter.getX, circle.getCenter.getY))
    gsf.createCircle()

  }

  val ST_BufferPoint: (Point, Double) => Polygon = (p, d) => {
    val degrees = DistanceUtils.dist2Degrees(d/1000.0, DistanceUtils.EARTH_MEAN_RADIUS_KM)
    fastCircleToPoly(new JtsPoint(p, spatialContext).getBuffered(degrees, spatialContext))
  }

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_bufferPoint"         , ST_BufferPoint)
  }

}
