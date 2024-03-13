/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sedona

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.jts.GeometryUDT
import org.locationtech.geomesa.spark.haveSedona
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral

import scala.util.Try

// Catalyst optimization rule for folding constant geometry expressions, such as
// ST_PointFromText('40.7128,-74.0060', ',') or st_makeBBOX(116.3, 39.90, 116.5, 40.1)
object SedonaGeometryLiteralRules extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case q: LogicalPlan => q.transformExpressionsDown {
        case expr: Expression if haveSedona && isSedonaExpression(expr) => tryConstantFolding(expr)
      }
    }
  }

  private def isSedonaExpression(expression: Expression): Boolean =
    expression.getClass.getCanonicalName.startsWith("org.apache.spark.sql.sedona_sql.expressions")

  private def tryConstantFolding(expr: Expression): Expression = {
    val attempt = Try {
      expr.eval(null) match {
        case data: ArrayData   => GeometryLiteral(data, GeometryUDT.deserialize(data))
        case data: Array[Byte] => GeometryLiteral(data, GeometryUDT.deserialize(data))
        case other: Any => Literal(other)
      }
    }
    attempt.getOrElse(expr)
  }
}
