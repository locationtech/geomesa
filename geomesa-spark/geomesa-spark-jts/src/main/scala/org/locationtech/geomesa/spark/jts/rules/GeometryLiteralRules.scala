/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.rules

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.jts._
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral._

import scala.reflect.ClassTag
import scala.util.Try

object GeometryLiteralRules {

  object ScalaUDFRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s: ScalaUDF if s.dataType.isInstanceOf[PointUDT] => eval(s, PointLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[LineStringUDT] => eval(s, LineStringLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[PolygonUDT] => eval(s, PolygonLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[GeometryUDT] => eval(s, GenericGeometryLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[MultiPointUDT] => eval(s, MultiPointLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[MultiLineStringUDT] => eval(s, MultiLineStringLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[MultiPolygonUDT] => eval(s, MultiPolygonLiteral.apply)
          case s: ScalaUDF if s.dataType.isInstanceOf[GeometryCollectionUDT] => eval(s, GeometryCollectionLiteral.apply)
        }
      }
    }

    private def eval[T: ClassTag](s: ScalaUDF, lit: T => Expression): Expression = {
      val t = Try {
        s.eval(null) match {
          case null => Literal(null)
          case t: T => lit(t)
          case a => Literal(a)
        }
      }
      t.getOrElse(s)
    }
  }

  private[jts] def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(ScalaUDFRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }
  }
}
