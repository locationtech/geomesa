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

  val GeometryTypeInstance           = new GeometryUDT
  val PointTypeInstance              = new PointUDT
  val LineStringTypeInstance         = new LineStringUDT
  val PolygonTypeInstance            = new PolygonUDT
  val MultiPointTypeInstance         = new MultiPointUDT
  val MultiLineStringTypeInstance    = new MultiLineStringUDT
  val MultipolygonTypeInstance       = new MultiPolygonUDT
  val GeometryCollectionTypeInstance = new GeometryCollectionUDT

  // these constant values conform to WKB values
  val GeometryType = 0
  val PointType = 1
  val LineStringType = 2
  val PolygonType = 3
  val MultiPointType = 4
  val MultiLineStringType= 5
  val MultiPolygonType = 6
  val GeometryCollectionType = 7

  UDTRegistration.register(classOf[Geometry].getCanonicalName, classOf[GeometryUDT].getCanonicalName)
  UDTRegistration.register(classOf[Point].getCanonicalName, classOf[PointUDT].getCanonicalName)
  UDTRegistration.register(classOf[LineString].getCanonicalName, classOf[LineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[Polygon].getCanonicalName, classOf[PolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiPoint].getCanonicalName, classOf[MultiPointUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiLineString].getCanonicalName, classOf[MultiLineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiPolygon].getCanonicalName, classOf[MultiPolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[GeometryCollection].getCanonicalName, classOf[GeometryCollectionUDT].getCanonicalName)

  def init(sqlContext: SQLContext): Unit = {
    SQLSpatialFunctions.registerFunctions(sqlContext)
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

private [spark] class GeometryUDT extends AbstractGeometryUDT[Geometry](0, "geometry") {
  override def serialize(obj: Geometry): InternalRow = {
    obj.getGeometryType match {
      case "Point"               => PointUDT.serialize(obj.asInstanceOf[Point])
      case "MultiPoint"          => MultiPointUDT.serialize(obj.asInstanceOf[MultiPoint])
      case "LineString"          => LineStringUDT.serialize(obj.asInstanceOf[LineString])
      case "LinearRing"          => LineStringUDT.serialize(obj.asInstanceOf[LineString])
      case "MultiLineString"     => MultiLineStringUDT.serialize(obj.asInstanceOf[MultiLineString])
      case "Polygon"             => PolygonUDT.serialize(obj.asInstanceOf[Polygon])
      case "MultiPolygon"        => MultiPolygonUDT.serialize(obj.asInstanceOf[MultiPolygon])
      case "GeometryCollection"  => GeometryCollectionUDT.serialize(obj.asInstanceOf[GeometryCollection])
    }
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  // TODO: Deal with Multi* serialization
  override def deserialize(datum: Any): Geometry = {
    val ir = datum.asInstanceOf[InternalRow]
    ir.getByte(0) match {
      case SQLTypes.PointType              => PointUDT.deserialize(ir)
      case SQLTypes.LineStringType         => LineStringUDT.deserialize(ir)
      case SQLTypes.PolygonType            => PolygonUDT.deserialize(ir)
      case SQLTypes.MultiPointType         => MultiPointUDT.deserialize(ir)
      case SQLTypes.MultiLineStringType    => MultiLineStringUDT.deserialize(ir)
      case SQLTypes.MultiPolygonType       => MultiPolygonUDT.deserialize(ir)
      case SQLTypes.GeometryCollectionType => GeometryCollectionUDT.deserialize(ir)
    }
  }

  private[sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
      dataType.getClass == SQLTypes.GeometryTypeInstance.getClass ||
      dataType.getClass == SQLTypes.PointTypeInstance.getClass ||
      dataType.getClass == SQLTypes.LineStringTypeInstance.getClass ||
      dataType.getClass == SQLTypes.PolygonTypeInstance.getClass ||
      dataType.getClass == SQLTypes.MultiLineStringTypeInstance.getClass ||
      dataType.getClass == SQLTypes.MultiPointTypeInstance.getClass ||
      dataType.getClass == SQLTypes.MultipolygonTypeInstance.getClass ||
      dataType.getClass == SQLTypes.GeometryCollectionTypeInstance.getClass
  }
}

case object GeometryUDT extends GeometryUDT

private [spark] class PointUDT
  extends AbstractGeometryUDT[Point](SQLTypes.PointType.toShort, "point")

object PointUDT extends PointUDT

private [spark] class LineStringUDT
  extends AbstractGeometryUDT[LineString](SQLTypes.LineStringType.toShort, "linestring")

object LineStringUDT extends LineStringUDT

private [spark] class PolygonUDT
  extends AbstractGeometryUDT[Polygon](SQLTypes.PolygonType.toShort, "polygon")

object PolygonUDT extends PolygonUDT

private [spark] class MultiPointUDT
  extends AbstractGeometryUDT[MultiPoint](SQLTypes.MultiLineStringType.toShort, "multipoint")

object MultiPointUDT extends MultiPointUDT

private [spark] class MultiLineStringUDT
  extends AbstractGeometryUDT[MultiLineString](SQLTypes.MultiLineStringType.toShort, "multilinestring")

object MultiLineStringUDT extends MultiLineStringUDT

private [spark] class MultiPolygonUDT
  extends AbstractGeometryUDT[MultiPolygon](SQLTypes.MultiPolygonType.toShort, "multipolygon")

object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class GeometryCollectionUDT
  extends AbstractGeometryUDT[GeometryCollection](SQLTypes.GeometryCollectionType.toShort, "geometrycollection")

object GeometryCollectionUDT extends GeometryCollectionUDT

