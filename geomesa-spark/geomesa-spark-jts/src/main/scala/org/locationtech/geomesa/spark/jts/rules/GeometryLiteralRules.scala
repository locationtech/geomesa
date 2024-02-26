/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.rules

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.jts.GeometryUDT

import scala.util.Try

object GeometryLiteralRules {

  object ScalaUDFRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s: ScalaUDF if s.function.getClass.getName.startsWith("org.locationtech.geomesa.spark") =>
            // TODO: Break down by GeometryType
            Try {
                s.eval(null) match {
                  // Prior to Spark 3.1.1 GenericInteralRows have been returned
                  // Spark 3.1.1 started returning UnsafeRows instead of GenericInteralRows
                  // Spark 3.5 returns primitive byte arrays
                  // When we're using serialization/deserialization functions provided by Apache Sedona in
                  // AbstractGeometryUDT, datum should be a GenericArrayData object.
                  case datum @ (_: InternalRow | _: ArrayData | _: Array[Byte]) =>
                    GeometryLiteral(datum, GeometryUDT.deserialize(datum))
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
