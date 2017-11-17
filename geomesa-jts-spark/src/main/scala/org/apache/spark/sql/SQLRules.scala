/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql


import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.SQLTypes._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GenericInternalRow, LeafExpression, Literal, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, StructType}
import org.opengis.filter.expression.{Expression => GTExpression}
import org.opengis.filter.{Filter => GTFilter}

import scala.collection.JavaConversions._
import scala.util.Try

object SQLRules {
  import SQLSpatialFunctions._

  def scalaUDFtoGTFilter(udf: Expression): Option[GTFilter] = {
    val ScalaUDF(func, _, expressions, _, _) = udf

    if (expressions.size == 2) {
      val Seq(exprA, exprB) = expressions
      buildGTFilter(func, exprA, exprB)
    } else {
      None
    }
  }

  private def buildGTFilter(func: AnyRef, exprA: Expression, exprB: Expression): Option[GTFilter] =
    for {
      builder <- funcToFF(func)
      gtExprA <- sparkExprToGTExpr(exprA)
      gtExprB <- sparkExprToGTExpr(exprB)
    } yield {
      builder(gtExprA, gtExprB)
    }

  def funcToFF(func: AnyRef): Option[(GTExpression, GTExpression) => GTFilter] = {
    func match {
      case ST_Contains => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.contains(expr1, expr2))
      case ST_Crosses => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.crosses(expr1, expr2))
      case ST_Disjoint => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.disjoint(expr1, expr2))
      case ST_Equals => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.equal(expr1, expr2))
      case ST_Intersects => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.intersects(expr1, expr2))
      case ST_Overlaps => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.overlaps(expr1, expr2))
      case ST_Touches => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.touches(expr1, expr2))
      case ST_Within => Some((expr1: GTExpression, expr2: GTExpression) =>
        ff.within(expr1, expr2))
      case _ => None
    }
  }

  def sparkExprToGTExpr(expr: org.apache.spark.sql.catalyst.expressions.Expression): Option[org.opengis.filter.expression.Expression] = {
    expr match {
      case GeometryLiteral(_, geom) =>
        Some(ff.literal(geom))
      case AttributeReference(name, _, _, _) =>
        Some(ff.property(name))
      case _ => None
    }
  }

  // new AST expressions
  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryTypeInstance
  }


  object ScalaUDFRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s@ScalaUDF(_, _, _, _, _) =>
            // TODO: Break down by GeometryType
            Try {
                s.eval(null) match {
                  case row: GenericInternalRow =>
                    val ret = GeometryUDT.deserialize(row)
                    GeometryLiteral(row, ret)
                  case other: Any =>
                    Literal(other)
                }
            }.getOrElse(s)
        }
      }
    }
  }



  def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(ScalaUDFRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }
  }
}
