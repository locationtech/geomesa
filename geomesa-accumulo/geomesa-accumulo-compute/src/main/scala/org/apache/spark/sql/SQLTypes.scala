package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, Literal, ScalaUDF, UnsafeArrayData}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.compute.spark.{GeoMesaRelation, GeoMesaSpark, SparkUtils}
import org.locationtech.geomesa.utils.text.WKTUtils

/**
  * Created by afox on 10/27/16.
  */

object SQLTypes {

  val geomFactory = JTSFactoryFinder.getGeometryFactory
  @transient val ff = CommonFactoryFinder.getFilterFactory2

  val PointType = new PointUDT

  UDTRegistration.register(classOf[Point].getCanonicalName, classOf[PointUDT].getCanonicalName)
  UDTRegistration.register(classOf[Polygon].getCanonicalName, classOf[PolygonUDT].getCanonicalName)


  val st_containsF: (Point, Polygon) => Boolean = (p, poly) => poly.contains(p)

  // TODO: optimize when used as a literal
  // e.g. select * from feature where st_contains(geom, geomFromText('POLYGON((....))'))
  // should not deserialize the POLYGON for every call
  val st_geomFromText: String => Polygon = s => WKTUtils.read(s).asInstanceOf[Polygon]

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geomFromText", st_geomFromText)
    sqlContext.udf.register("st_contains",     st_containsF)
  }


  case class STContainsPlan(@transient poly: UTF8String, @transient rel: GeoMesaRelation, @transient child: SparkPlan) extends SparkPlan {
    import scala.collection.JavaConversions._

    override protected def doExecute(): RDD[InternalRow] = {
      val filt = ff.within(ff.property(rel.sft.getGeometryDescriptor.getLocalName), ff.literal(WKTUtils.read(poly.toString)))
      val rdd = GeoMesaSpark.rdd(new Configuration(), sqlContext.sparkContext, rel.params, new Query(rel.params("geomesa.feature"), filt), Option.empty[Int])
      val ranges = (0 until rel.sft.getAttributeCount).toArray
      val schema = rel.schema // capture property of transient
      val rows = rdd.map { sf => sf.getAttributes.map(SparkUtils.toSparkType) }.map { s => Row.fromSeq(s) }
      RDDConversions.rowToRowRdd(rows, rel.schema.fields.map(_.dataType))
    }

    override def output: Seq[Attribute] = child.output

    override def children: Seq[SparkPlan] = Nil
  }

  object STContainsStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Filter(condition @ ScalaUDF(fn, DataTypes.BooleanType, Seq(_, ScalaUDF(_, _, Seq(Literal(poly: UTF8String, DataTypes.StringType)), _)), _), child @ LogicalRelation(gmRel: GeoMesaRelation, _, _)) if fn.equals(st_containsF) => {
        Seq(STContainsPlan(poly, gmRel, planLater(child)))
      }
      case _ => {
        Nil
      }
    }
  }

  object STContainsRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan
    }
  }

  def registerStrategies(sqlContext: SQLContext): Unit = {
    sqlContext.experimental.extraStrategies = Seq(STContainsStrategy)
    sqlContext.experimental.extraOptimizations = Seq(STContainsRule)
  }

  def init(sqlContext: SQLContext): Unit = {
    registerFunctions(sqlContext)
    registerStrategies(sqlContext)
  }
}

private [spark] class PointUDT extends UserDefinedType[Point] {

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
    new GenericInternalRow(Array(1.asInstanceOf[Byte],
      UnsafeArrayData.fromPrimitiveArray(coords)))
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val ir = datum.asInstanceOf[InternalRow]
    val coords = ir.getArray(1).toDoubleArray().grouped(2).map { case Array(l, r) => new Coordinate(l, r) }
    SQLTypes.geomFactory.createPolygon(coords.toArray)
  }

}
