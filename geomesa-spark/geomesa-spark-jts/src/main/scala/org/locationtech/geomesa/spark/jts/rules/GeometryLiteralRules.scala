/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.rules

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.jts.GeometryUDT

import scala.util.Try

object GeometryLiteralRules {

  object ScalaUDFRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s: ScalaUDF =>
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

  private[jts] def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(ScalaUDFRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }
  }
}
