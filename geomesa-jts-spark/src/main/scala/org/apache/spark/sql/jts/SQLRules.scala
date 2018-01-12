/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql.jts

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, LeafExpression, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.jts.SQLTypes._
import scala.util.Try

object SQLRules {

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
