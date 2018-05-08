/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.{lang => jl}

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression}
import org.apache.spark.sql.jts.{AbstractGeometryUDT, GeometryUDT}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.util.WKBUtils

object GeometricAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = nullableUDF(geom => geom.getBoundary)
  val ST_CoordDim: Geometry => Int = nullableUDF(geom => {
    val coord = geom.getCoordinate
    if (coord.z.isNaN) 2 else 3
  })
  val ST_Dimension: Geometry => Int = nullableUDF(geom => geom.getDimension)
  val ST_Envelope: Geometry => Geometry = nullableUDF(geom => geom.getEnvelope)
  val ST_ExteriorRing: Geometry => LineString = {
    case geom: Polygon => geom.getExteriorRing
    case _ => null
  }
  val ST_GeometryN: (Geometry, Int) => Geometry = nullableUDF((geom, n) => geom.getGeometryN(n-1))
  val ST_GeometryType: Geometry => String = nullableUDF(geom => geom.getGeometryType)
  val ST_InteriorRingN: (Geometry, Int) => Geometry = nullableUDF((geom, n) => {
    geom match {
      case geom: Polygon =>
        if (0 < n && n <= geom.getNumInteriorRing) {
          geom.getInteriorRingN(n-1)
        } else {
          null
        }
      case _ => null
    }
  })
  val ST_IsClosed: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed
    case geom: MultiLineString => geom.isClosed
    case _ => true
  })
  val ST_IsCollection: Geometry => jl.Boolean = nullableUDF(geom => geom.isInstanceOf[GeometryCollection])
  val ST_IsEmpty: Geometry => jl.Boolean = nullableUDF(geom => geom.isEmpty)
  val ST_IsRing: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed && geom.isSimple
    case geom: MultiLineString => geom.isClosed && geom.isSimple
    case geom => geom.isSimple
  })
  val ST_IsSimple: Geometry => jl.Boolean = nullableUDF(geom => geom.isSimple)
  val ST_IsValid: Geometry => jl.Boolean = nullableUDF(geom => geom.isValid)
  val ST_NumGeometries: Geometry => Int = nullableUDF(geom => geom.getNumGeometries)
  val ST_NumPoints: Geometry => Int = nullableUDF(geom => geom.getNumPoints)
  val ST_PointN: (Geometry, Int) => Point = nullableUDF((geom, n) => {
    geom match {
      case geom: LineString =>
        if (n < 0) {
          geom.getPointN(n + geom.getLength.toInt)
        } else {
          geom.getPointN(n-1)
        }
      case _ => null
    }
  })
  val ST_X: Geometry => jl.Float = {
    case geom: Point => geom.getX.toFloat
    case _ => null
  }
  val ST_Y: Geometry => jl.Float = {
    case geom: Point => geom.getY.toFloat
    case _ => null
  }

  private[geomesa] val accessorNames = Map(
    ST_Boundary -> "st_boundary",
    ST_CoordDim -> "st_coordDim",
    ST_Dimension -> "st_dimension",
    ST_Envelope -> "st_envelope",
    ST_ExteriorRing -> "st_exteriorRing",
    ST_GeometryN -> "st_geometryN",
    ST_GeometryType -> "st_geometryType",
    ST_InteriorRingN -> "st_interiorRingN",
    ST_IsClosed -> "st_isClosed",
    ST_IsCollection -> "st_isCollection",
    ST_IsEmpty -> "st_isEmpty",
    ST_IsRing -> "st_isRing",
    ST_IsSimple -> "st_isSimple",
    ST_IsValid -> "st_isValid",
    ST_NumGeometries -> "st_numGeometries",
    ST_NumPoints -> "st_numPoints",
    ST_PointN -> "st_pointN",
    ST_X -> "st_x",
    ST_Y -> "st_y"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(accessorNames(ST_Boundary), ST_Boundary)
    sqlContext.udf.register(accessorNames(ST_CoordDim), ST_CoordDim)
    sqlContext.udf.register(accessorNames(ST_Dimension), ST_Dimension)
    sqlContext.udf.register(accessorNames(ST_Envelope), ST_Envelope)
    sqlContext.udf.register(accessorNames(ST_ExteriorRing), ST_ExteriorRing)
    sqlContext.udf.register(accessorNames(ST_GeometryN), ST_GeometryN)
    sqlContext.udf.register(accessorNames(ST_GeometryType), ST_GeometryType)
    sqlContext.udf.register(accessorNames(ST_InteriorRingN), ST_InteriorRingN)
    sqlContext.udf.register(accessorNames(ST_IsClosed), ST_IsClosed)
    sqlContext.udf.register(accessorNames(ST_IsCollection), ST_IsCollection)
    sqlContext.udf.register(accessorNames(ST_IsEmpty), ST_IsEmpty)
    sqlContext.udf.register(accessorNames(ST_IsRing), ST_IsRing)
    sqlContext.udf.register(accessorNames(ST_IsSimple), ST_IsSimple)
    sqlContext.udf.register(accessorNames(ST_IsValid), ST_IsValid)
    sqlContext.udf.register(accessorNames(ST_NumGeometries), ST_NumGeometries)
    sqlContext.udf.register(accessorNames(ST_NumPoints), ST_NumPoints)
    sqlContext.udf.register(accessorNames(ST_PointN), ST_PointN)
    sqlContext.udf.register(accessorNames(ST_X), ST_X)
    sqlContext.udf.register(accessorNames(ST_Y), ST_Y)

    val fr = org.apache.spark.sql.jts.registry(sqlContext)
    fr.registerFunction("st_boundaryExpression", f => BoundaryExpression(f.head))
    fr.registerFunction("st_containsExpression", f => ContainsExpression(f(0), f(1)))
    fr.registerFunction("st_containsExpressionWithCG", f => ContainsExpressionWithCodeGen(f(0), f(1)))
  }
}

case class BoundaryExpression(child: Expression) extends UnaryExpression {
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode ={
    ctx.addMutableState(classOf[GeometryUDT].getName, "geometryUDT", "geometryUDT = new org.apache.spark.sql.jts.GeometryUDT();")

    dataType match {
      case geom: GeometryUDT  => defineCodeGen(ctx, ev, g => {
        s"geometryUDT.serialize(geometryUDT.deserialize($g).getBoundary())"
      })
    }
  }

  override def dataType: DataType = child.dataType
}

case class ContainsExpression(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback
  with GeomDeserializerSupport {
  //  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
  //    defineCodeGen(ctx, ev, (c1, c2) => {
  //      ctx.addMutableState(classOf[GeometryUDT].getName, "geometryUDT",
  //        "geometryUDT = new org.apache.spark.sql.jts.GeometryUDT();")
  //     s"geometryUDT.deserialize($c1).contains(geometryUDT.deserialize($c2))"
//
//    })

  override def toString: String = s"contains($left, $right)"
  override def nodeName: String = "contains"

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    extractGeometry(input1).contains(extractGeometry(input2))
  }

  override def dataType: DataType = BooleanType
}

trait GeomDeserializerSupport {
  def extractGeometry(datum: Any): Geometry = {
    val ir = datum.asInstanceOf[InternalRow]
    WKBUtils.read(ir.getBinary(0)).asInstanceOf[Geometry]
  }
}

case class ContainsExpressionWithCodeGen(left: Expression, right: Expression) extends BinaryExpression {
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (c1, c2) => {
      ctx.addMutableState(classOf[GeometryUDT].getName, "geometryUDT",
        "geometryUDT = new org.apache.spark.sql.jts.GeometryUDT();")
      s"geometryUDT.deserialize($c1).contains(geometryUDT.deserialize($c2))"

    })

  override def dataType: DataType = BooleanType
}