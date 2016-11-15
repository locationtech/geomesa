package org.apache.spark.sql

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.locationtech.geomesa.compute.spark.GeoMesaRelation
import org.locationtech.geomesa.utils.text.WKTUtils
import org.slf4j.LoggerFactory

class GeoMesaSQL

object SQLTypes {

  @transient val log = LoggerFactory.getLogger(classOf[GeoMesaSQL])
  @transient val geomFactory = JTSFactoryFinder.getGeometryFactory
  @transient val ff = CommonFactoryFinder.getFilterFactory2

  val PointType       = new PointUDT
  val LineStringType  = new LineStringUDT
  val GeometryType    = new GeometryUDT

  UDTRegistration.register(classOf[Point].getCanonicalName, classOf[PointUDT].getCanonicalName)
  UDTRegistration.register(classOf[LineString].getCanonicalName, classOf[LineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[Polygon].getCanonicalName, classOf[PolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[Geometry].getCanonicalName, classOf[GeometryUDT].getCanonicalName)

  val ST_Contains: (Point, Geometry) => Boolean = (p, geom) => geom.contains(p)
  val ST_Envelope:  Geometry => Geometry = p => p.getEnvelope
  val ST_MakeBox2D: (Point, Point) => Polygon = (ll, ur) => JTS.toGeometry(new Envelope(ll.getX, ur.getX, ll.getY, ur.getY))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Polygon = (lx, ly, ux, uy) => JTS.toGeometry(new Envelope(lx, ux, ly, uy))
  val ST_Centroid: Geometry => Point = g => g.getCentroid

  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]

  // TODO: optimize when used as a literal
  // e.g. select * from feature where st_contains(geom, geomFromText('POLYGON((....))'))
  // should not deserialize the POLYGON for every call
  val ST_GeomFromWKT: String => Geometry = s => WKTUtils.read(s)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geomFromWKT"   , ST_GeomFromWKT)
    sqlContext.udf.register("st_contains"      , ST_Contains)
    sqlContext.udf.register("st_within"        , ST_Contains) // TODO: is contains different than within?
    sqlContext.udf.register("st_envelope"      , ST_Envelope)
    sqlContext.udf.register("st_makeBox2D"     , ST_MakeBox2D)
    sqlContext.udf.register("st_makeBBOX"      , ST_MakeBBOX)
    sqlContext.udf.register("st_centroid"      , ST_Centroid)
    sqlContext.udf.register("st_castToPoint"   , ST_CastToPoint)
  }

  // new AST expressions
  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryType

  }

  // new optimizations rules
 object STContainsRule extends Rule[LogicalPlan] with PredicateHelper {

    def extractGeometry(e: Expression): Option[Geometry] = e match {
       case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
       case ScalaUDF(ST_Contains, _, Seq(_, GeometryLiteral(_, geom)), _) => Some(geom)
       case _ => None  
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case filt @ Filter(f, lr@LogicalRelation(gmRel: GeoMesaRelation, _, _)) =>
          // TODO: deal with `or`

          // split up conjunctive predicates and extract the st_contains variable
          val (st_contains, xs) = splitConjunctivePredicates(f).partition {
            case ScalaUDF(ST_Contains, _, _, _) => true
            case _                              => false
          }
          if(st_contains.nonEmpty) {
            // we got an st_contains, extract the geometry and set up the new GeoMesa relation with the appropriate
            // CQL filter

            // TODO: only dealing with one st_contains at the moment
            val ScalaUDF(_, _, Seq(_, GeometryLiteral(_, geom)), _) = st_contains.head
            log.debug("Optimizing 'st_contains'")
            val geomDescriptor = gmRel.sft.getGeometryDescriptor.getLocalName
            val cqlFilter = ff.within(ff.property(geomDescriptor), ff.literal(geom))
            val relation = gmRel.copy(filt = ff.and(gmRel.filt, cqlFilter))
            // need to maintain expectedOutputAttributes so identifiers don't change in projections
            val newrel = lr.copy(expectedOutputAttributes = Some(lr.output), relation = relation)
            if(xs.nonEmpty) {
              // if there are other filters, keep them
              Filter(xs.reduce(And), newrel)
            } else {
              // if st_contains was the only filter, just return the new relation
              newrel
            }
          } else {
            filt
          }
      }
    }
  }

  object FoldConstantGeometryRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case ScalaUDF(ST_GeomFromWKT, GeometryType, Seq(Literal(wkt, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
            val geom = ST_GeomFromWKT(wkt.asInstanceOf[UTF8String].toString)
            GeometryLiteral(GeometryUDT.serialize(geom), geom)
        }
      }
    }
  }

  def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(FoldConstantGeometryRule, STContainsRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }

    Seq.empty[Strategy].foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }

  def init(sqlContext: SQLContext): Unit = {
    registerFunctions(sqlContext)
    registerOptimizations(sqlContext)
  }
}

private [spark] class PointUDT extends UserDefinedType[Point] {

  override def simpleString: String = "point"

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Point): InternalRow = {
    new GenericInternalRow(Array(1.asInstanceOf[Byte], UnsafeArrayData.fromPrimitiveArray(Array(obj.getX, obj.getY))))
  }

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    val ir = datum.asInstanceOf[InternalRow]
    val coords = ir.getArray(1).toDoubleArray()
    SQLTypes.geomFactory.createPoint(new Coordinate(coords(0), coords(1)))
  }
}

object PointUDT extends PointUDT

private [spark] class LineStringUDT extends UserDefinedType[LineString] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: LineString): InternalRow = {
    // only simple polys for now
    val coords = obj.getCoordinates.map { c => Array(c.x, c.y) }.reduce { (l, r) => l ++ r }
    new GenericInternalRow(Array(2.asInstanceOf[Byte],
      UnsafeArrayData.fromPrimitiveArray(coords)))
  }

  override def userClass: Class[LineString] = classOf[LineString]

  override def deserialize(datum: Any): LineString = {
    val ir = datum.asInstanceOf[InternalRow]
    val coords = ir.getArray(2).toDoubleArray().grouped(2).map { case Array(l, r) => new Coordinate(l, r) }
    SQLTypes.geomFactory.createLineString(coords.toArray)
  }
}

object LineStringUDT extends LineStringUDT

private [spark] class PolygonUDT extends UserDefinedType[Polygon] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Polygon): InternalRow = {
    // only simple polys for now
    val coords = obj.getCoordinates.map { c => Array(c.x, c.y) }.reduce { (l, r) => l ++ r }
    new GenericInternalRow(Array(3.asInstanceOf[Byte],
      UnsafeArrayData.fromPrimitiveArray(coords)))
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val ir = datum.asInstanceOf[InternalRow]
    val coordsD = ir.getArray(1).toDoubleArray()
    val length = coordsD.length
    val numCoords = length/2
    val coords = Array.ofDim[Coordinate](numCoords)
    var i = 0
    while(i < numCoords) {
      val offset = i*2
      coords(i) = new Coordinate(coordsD(offset), coordsD(offset+1))
      i += 1
    }
    SQLTypes.geomFactory.createPolygon(coords)
  }

}

object PolygonUDT extends PolygonUDT

private [spark] class GeometryUDT extends UserDefinedType[Geometry] {


  override def simpleString: String = "geometry"

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Geometry): InternalRow = {
    obj.getGeometryType match {
      case "Point"      => PointUDT.serialize(obj.asInstanceOf[Point])
      case "LineString" => LineStringUDT.serialize(obj.asInstanceOf[LineString])
      case "Polygon"    => PolygonUDT.serialize(obj.asInstanceOf[Polygon])
    }
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def deserialize(datum: Any): Geometry = {
    val ir = datum.asInstanceOf[InternalRow]
    ir.getByte(0) match {
      case 1 => PointUDT.deserialize(ir)
      case 2 => LineStringUDT.deserialize(ir)
      case 3 => PolygonUDT.deserialize(ir)
    }
  }
}

case object GeometryUDT extends GeometryUDT
