package geomesa.core.index

import com.vividsolutions.jts.geom.{Point, MultiPolygon, Polygon, Geometry}
import geomesa.core.filter._
import geomesa.utils.geohash.GeohashUtils._
import geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._
import scala.collection.JavaConversions._

trait FilterHelper {
  def featureType: SimpleFeatureType

  def visitBinarySpatialOp(op: BinarySpatialOperator): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val geom = e2.evaluate(null, classOf[Geometry])
    val safeGeometry = getInternationalDateLineSafeGeometry(geom)
    updateToIDLSafeFilter(op, safeGeometry)
  }

  def visitBBOX(op: BBOX): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val geom = addWayPointsToBBOX( e2.evaluate(null, classOf[Geometry]) )
    val safeGeometry = getInternationalDateLineSafeGeometry(geom)
    updateToIDLSafeFilter(op, safeGeometry)
  }


  def updateToIDLSafeFilter(op: BinarySpatialOperator, geom: Geometry): Filter = geom match {
    case p: Polygon =>
      doCorrectSpatialCall(op, featureType.getGeometryDescriptor.getLocalName, p) //op
    case mp: MultiPolygon =>
      val polygonList = getGeometryListOf(geom)
      val filterList = polygonList.map {
        p => doCorrectSpatialCall(op, featureType.getGeometryDescriptor.getLocalName, p)
      }
      ff.or(filterList)
  }

  def getGeometryListOf(inMP: Geometry): Seq[Geometry] =
    for( i <- 0 until inMP.getNumGeometries ) yield inMP.getGeometryN(i)

  def doCorrectSpatialCall(op: BinarySpatialOperator, property: String, geom: Geometry): Filter = op match {
    case op: Within     => ff.within( ff.property(property), ff.literal(geom) )
    case op: Intersects => ff.intersects( ff.property(property), ff.literal(geom) )
    case op: Overlaps   => ff.overlaps( ff.property(property), ff.literal(geom) )
    case op: BBOX       => val envelope = geom.getEnvelopeInternal
      ff.bbox( ff.property(property), envelope.getMinX, envelope.getMinY,
        envelope.getMaxX, envelope.getMaxY, op.getSRS )
  }

  def addWayPointsToBBOX(g: Geometry):Geometry = {
    val gf = g.getFactory
    val geomArray = g.getCoordinates
    val correctedGeom = GeometryUtils.addWayPoints(geomArray).toArray
    gf.createPolygon(correctedGeom)
  }

  // Rewrites a Dwithin (assumed to express distance in meters) in degrees.
  def rewriteDwithin(op: DWithin): Filter = {
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val startPoint = e2.evaluate(null, classOf[Point])
    val distance = op.getDistance
    val distanceDegrees = GeometryUtils.distanceDegrees(startPoint, distance)

    ff.dwithin(
      op.getExpression1,
      ff.literal(startPoint),
      distanceDegrees,
      "meters")
  }


}
