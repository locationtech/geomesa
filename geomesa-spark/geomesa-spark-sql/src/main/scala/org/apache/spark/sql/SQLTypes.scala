/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.filter.FilterFactory2

import scala.reflect.ClassTag

object SQLTypes {

  @transient val geomFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory
  @transient val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

  val PointType             = new PointUDT
  val MultiPointType        = new MultiPointUDT
  val LineStringType        = new LineStringUDT
  val MultiLineStringType   = new MultiLineStringUDT
  val PolygonType           = new PolygonUDT
  val MultipolygonType      = new MultiPolygonUDT
  val GeometryType          = new GeometryUDT
  // TODO: Implement GeometryCollectionType

  UDTRegistration.register(classOf[Point].getCanonicalName, classOf[PointUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiPoint].getCanonicalName, classOf[MultiPointUDT].getCanonicalName)
  UDTRegistration.register(classOf[LineString].getCanonicalName, classOf[LineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiLineString].getCanonicalName, classOf[MultiLineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[Polygon].getCanonicalName, classOf[PolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiPolygon].getCanonicalName, classOf[MultiPolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[Geometry].getCanonicalName, classOf[GeometryUDT].getCanonicalName)

  def init(sqlContext: SQLContext): Unit = {
    SQLSpatialFunctions.registerFunctions(sqlContext)
    SQLRules.registerOptimizations(sqlContext)
  }
}

abstract class AbstractGeometryUDT[T >: Null <: Geometry](override val simpleString: String)(implicit cm: ClassTag[T])
  extends UserDefinedType[T] {
  override def serialize(obj: T): InternalRow = {
    new GenericInternalRow(Array[Any](WKBUtils.write(obj)))
  }
  override def sqlType: DataType = StructType(
    Seq(
      StructField("wkb", DataTypes.BinaryType)
    )
  )

  override def userClass: Class[T] = cm.runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = {
    val ir = datum.asInstanceOf[InternalRow]
    WKBUtils.read(ir.getBinary(0)).asInstanceOf[T]
  }
}

private [spark] class PointUDT extends AbstractGeometryUDT[Point]("point")
object PointUDT extends PointUDT

private [spark] class MultiPointUDT extends AbstractGeometryUDT[MultiPoint]("multipoint")
object MultiPointUDT extends MultiPointUDT

private [spark] class LineStringUDT extends AbstractGeometryUDT[LineString]("linestring")
object LineStringUDT extends LineStringUDT

private [spark] class MultiLineStringUDT extends AbstractGeometryUDT[MultiLineString]("multilinestring")
object MultiLineStringUDT extends MultiLineStringUDT

private [spark] class PolygonUDT extends AbstractGeometryUDT[Polygon]("polygon")
object PolygonUDT extends PolygonUDT

private [spark] class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]("multipolygon")
object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class GeometryUDT extends AbstractGeometryUDT[Geometry]("geometry") {
  private[sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
      dataType.getClass == SQLTypes.PointType.getClass ||
      dataType.getClass == SQLTypes.MultiPointType.getClass ||
      dataType.getClass == SQLTypes.LineStringType.getClass ||
      dataType.getClass == SQLTypes.MultiLineStringType.getClass ||
      dataType.getClass == SQLTypes.PolygonType.getClass ||
      dataType.getClass == SQLTypes.MultipolygonType.getClass ||
      dataType.getClass == SQLTypes.GeometryType.getClass
  }
}

case object GeometryUDT extends GeometryUDT
