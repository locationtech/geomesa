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

import scala.reflect.ClassTag

object SQLTypes {

  @transient val geomFactory = JTSFactoryFinder.getGeometryFactory
  @transient val ff = CommonFactoryFinder.getFilterFactory2

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
    SQLCastFunctions.registerFunctions(sqlContext)
    SQLSpatialFunctions.registerFunctions(sqlContext)
    SQLGeometricConstructorFunctions.registerFunctions(sqlContext)
    SQLRules.registerOptimizations(sqlContext)
  }
}

abstract class AbstractGeometryUDT[T >: Null <: Geometry](id: Short, override val simpleString: String)(implicit cm: ClassTag[T])
  extends UserDefinedType[T] {
  override def serialize(obj: T): InternalRow = {
    new GenericInternalRow(Array(1.asInstanceOf[Byte], WKBUtils.write(obj)))
  }
  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.BinaryType)
    )
  )

  override def userClass: Class[T] = cm.runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = {
    val ir = datum.asInstanceOf[InternalRow]
    WKBUtils.read(ir.getBinary(1)).asInstanceOf[T]
  }
}

private [spark] class PointUDT extends AbstractGeometryUDT[Point](1, "point")
object PointUDT extends PointUDT

private [spark] class MultiPointUDT extends AbstractGeometryUDT[MultiPoint](4, "multipoint")
object MultiPointUDT extends MultiPointUDT

private [spark] class LineStringUDT extends AbstractGeometryUDT[LineString](2, "linestring")
object LineStringUDT extends LineStringUDT

private [spark] class MultiLineStringUDT extends AbstractGeometryUDT[MultiLineString](5, "multilinestring")
object MultiLineStringUDT extends MultiLineStringUDT

private [spark] class PolygonUDT extends AbstractGeometryUDT[Polygon](3, "polygon")
object PolygonUDT extends PolygonUDT

private [spark] class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon](6, "multipolygon")
object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class GeometryUDT extends AbstractGeometryUDT[Geometry](0, "geometry") {
  override def serialize(obj: Geometry): InternalRow = {
    obj.getGeometryType match {
      case "Point"               => PointUDT.serialize(obj.asInstanceOf[Point])
      case "MultiPoint"          => MultiPointUDT.serialize(obj.asInstanceOf[MultiPoint])
      case "LineString"          => LineStringUDT.serialize(obj.asInstanceOf[LineString])
      case "MultiLineString"     => MultiLineStringUDT.serialize(obj.asInstanceOf[MultiLineString])
      case "Polygon"             => PolygonUDT.serialize(obj.asInstanceOf[Polygon])
      case "MultiPolygon"        => MultiPolygonUDT.serialize(obj.asInstanceOf[MultiPolygon])
    }
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  // TODO: Deal with Multi* serialization
  override def deserialize(datum: Any): Geometry = {
    val ir = datum.asInstanceOf[InternalRow]
    ir.getByte(0) match {
      case 1 => PointUDT.deserialize(ir)
      case 2 => LineStringUDT.deserialize(ir)
      case 3 => PolygonUDT.deserialize(ir)
      case 4 => MultiPointUDT.deserialize(ir)
      case 5 => MultiLineStringUDT.deserialize(ir)
      case 6 => MultiPolygonUDT.deserialize(ir)
    }
  }

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
