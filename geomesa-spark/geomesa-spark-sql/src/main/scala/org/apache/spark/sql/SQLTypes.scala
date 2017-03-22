/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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

  val GeometryTypeInstance           = new GeometryUDT
  val PointTypeInstance              = new PointUDT
  val LineStringTypeInstance         = new LineStringUDT
  val PolygonTypeInstance            = new PolygonUDT
  val MultiPointTypeInstance         = new MultiPointUDT
  val MultiLineStringTypeInstance    = new MultiLineStringUDT
  val MultipolygonTypeInstance       = new MultiPolygonUDT
  val GeometryCollectionTypeInstance = new GeometryCollectionUDT

  // these constant values conform to WKB values
  val GeometryType           = 0
  val PointType              = 1
  val LineStringType         = 2
  val PolygonType            = 3
  val MultiPointType         = 4
  val MultiLineStringType    = 5
  val MultiPolygonType       = 6
  val GeometryCollectionType = 7

  val typeMap = Map[Class[_], Class[_ <: UserDefinedType[_]]](
    classOf[Geometry]            -> classOf[GeometryUDT],
    classOf[Point]               -> classOf[PointUDT],
    classOf[LineString]          -> classOf[LineStringUDT],
    classOf[Polygon]             -> classOf[PolygonUDT],
    classOf[MultiPoint]          -> classOf[MultiPointUDT],
    classOf[MultiLineString]     -> classOf[MultiLineStringUDT],
    classOf[MultiPolygon]        -> classOf[MultiPolygonUDT],
    classOf[GeometryCollection]  -> classOf[GeometryCollectionUDT]
  )

  typeMap.foreach { case (l, r) => UDTRegistration.register(l.getCanonicalName, r.getCanonicalName) }

  def init(sqlContext: SQLContext): Unit = {
    SQLGeometricCastFunctions.registerFunctions(sqlContext)
    SQLSpatialAccessorFunctions.registerFunctions(sqlContext)
    SQLSpatialFunctions.registerFunctions(sqlContext)
    SQLGeometricConstructorFunctions.registerFunctions(sqlContext)
    SQLGeometryProcessingFunctions.registerFunctions(sqlContext)
    SQLGeometricOutputFunctions.registerFunctions(sqlContext)
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

private [spark] class GeometryCollectionUDT
  extends AbstractGeometryUDT[GeometryCollection]("geometrycollection")

object GeometryCollectionUDT extends GeometryCollectionUDT

