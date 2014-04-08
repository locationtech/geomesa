package geomesa.core.data

import collection.JavaConversions._
import com.vividsolutions.jts.geom._
import geomesa.core.index
import geomesa.utils.geotools.Conversions._
import geomesa.utils.geotools.GeometryUtils
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.geotools.geometry.jts.{JTSFactoryFinder, JTS}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, Interval}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.Instant
import geomesa.utils.text.WKTUtils
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence

// FilterToAccumulo extracts the spatial and temporal predicates from the
// filter while rewriting the filter to optimize for scanning Accumulo
class FilterToAccumulo(sft: SimpleFeatureType) {

  val dtgField  = index.getDtgDescriptor(sft)
  val geomField = sft.getGeometryDescriptor

  val allTime            = new Interval(0, Long.MaxValue)
  val wholeWorld         = new Envelope(-180, -90, 180, 90)

  var spatialPredicate:  Polygon  = null
  var temporalPredicate: Interval    = null

  val ff = CommonFactoryFinder.getFilterFactory2
  val geoFactory = JTSFactoryFinder.getGeometryFactory

  def visit(query: Query): Filter = visit(query.getFilter)
  def visit(filter: Filter): Filter =
    process(filter).accept(new SimplifyingFilterVisitor, null).asInstanceOf[Filter]

  def processChildren(op: BinaryLogicOperator, lf: (Filter, Filter) => Filter): Filter =
    op.getChildren.reduce { (l, r) => lf(process(l), process(r)) }
  
  def process(filter: Filter, acc: Filter = Filter.INCLUDE): Filter = filter match {
    // Logical filters
    case op: Or    => processChildren(op, ff.or)
    case op: And   => processChildren(op, ff.and)

    // Spatial filters
    case op: BBOX       => visitBBOX(op, acc)
    case op: DWithin    => visitDWithin(op, acc)
    case op: Within     => visitBinarySpatialOp(op, acc)
    case op: Intersects => visitBinarySpatialOp(op, acc)
    case op: Overlaps   => visitBinarySpatialOp(op, acc)

    // Temporal filters
    case op: BinaryTemporalOperator => visitBinaryTemporalOp(op, acc)

    // Other
    case op: PropertyIsBetween      => visitPropertyIsBetween(op, acc)

    // Catch all
    case f: Filter => ff.and(acc, f)
  }

  private def visitBBOX(op: BBOX, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      spatialPredicate = JTS.toGeometry(op.getBounds)
      acc
    }
  }

  private def visitBinarySpatialOp(op: BinarySpatialOperator, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val geom = e2.evaluate(null, classOf[Geometry])
      spatialPredicate = geom.asInstanceOf[Polygon]
      if(!geom.isRectangle) ff.and(acc, op)
      else acc
    }
  }

  def visitDWithin(op: DWithin, acc: Filter): Filter = {
    val e1 = op.getExpression1.asInstanceOf[PropertyName]
    val e2 = op.getExpression2.asInstanceOf[Literal]
    val attr = e1.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(sft.getGeometryDescriptor.getLocalName)) {
      ff.and(acc, op)
    } else {
      val geoCalc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
      val startPoint = e2.evaluate(null, classOf[Point])
      val distance = op.getDistance

      // Convert meters to dec degrees based on widest point in dec degrees of circle
      geoCalc.setStartingGeographicPoint(startPoint.getX, startPoint.getY)
      geoCalc.setDirection(90, distance)
      val right = geoCalc.getDestinationGeographicPoint
      val distanceDegrees = startPoint.distance(geoFactory.createPoint(new Coordinate(right.getX, right.getY)))

      // Walk circle bounds for bounding box
      spatialPredicate = GeometryUtils.bufferPoint(startPoint, distance)

      val rewrittenFilter =
        ff.dwithin(
          ff.property(sft.getGeometryDescriptor.getLocalName),
          ff.literal(startPoint),
          distanceDegrees,
          "meters")
      ff.and(acc, rewrittenFilter)
    }
  }

  def visitBinaryTemporalOp(bto: BinaryTemporalOperator, acc: Filter): Filter = {
    val prop     = bto.getExpression1.asInstanceOf[PropertyName]
    val lit      = bto.getExpression2.asInstanceOf[Literal]
    val attr     = prop.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(dtgField.getLocalName)) ff.and(acc, bto)
    else {
      val period = lit.evaluate(null).asInstanceOf[org.opengis.temporal.Period]
      temporalPredicate = bto match {
        case op: Before    => new Interval(new DateTime(0L), period.getEnding)
        case op: After     => new Interval(period.getBeginning, new DateTime(Long.MaxValue))
        case op: During    => new Interval(period.getBeginning, period.getEnding)
        case op: TContains => new Interval(period.getBeginning, period.getEnding)
        case _             => throw new IllegalArgumentException("Invalid query")
      }
      acc
    }
  }

  def visitPropertyIsBetween(op: PropertyIsBetween, acc: Filter): Filter = {
    val prop = op.getExpression.asInstanceOf[PropertyName]
    val attr = prop.evaluate(sft).asInstanceOf[AttributeDescriptor]
    if(!attr.getLocalName.equals(dtgField.getLocalName)) ff.and(acc, op)
    else {
      val start = op.getLowerBoundary.evaluate(null).asInstanceOf[Instant]
      val end   = op.getUpperBoundary.evaluate(null).asInstanceOf[Instant]
      temporalPredicate = new Interval(start, end)
      acc
    }
  }

}
