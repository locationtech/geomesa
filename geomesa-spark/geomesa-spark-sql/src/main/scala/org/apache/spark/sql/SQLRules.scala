/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.SQLTypes._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, LeafExpression, Literal, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.geomesa.spark.GeoMesaRelation
import org.opengis.filter.expression.{Expression => GTExpression}

object SQLRules {
  // new AST expressions
  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryTypeInstance
  }

  // new optimizations rules
  object STContainsRule extends Rule[LogicalPlan] with PredicateHelper {
    import SQLSpatialFunctions._

    // JNH: NB: Unused.
    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
      case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
      case ScalaUDF(ST_Contains, _, Seq(_, GeometryLiteral(_, geom)), _) => Some(geom)
      case _ => None
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case sort @ Sort(_, _, _) => sort    // No-op.  Just realizing what we can do:)
        case filt @ Filter(f, lr@LogicalRelation(gmRel: GeoMesaRelation, _, _)) =>
          // TODO: deal with `or`

          // split up conjunctive predicates and extract the st_contains variable
          val (st_contains, xs) = splitConjunctivePredicates(f).partition {
            // TODO: Add guard which checks to see if the function can be pushed down
            case ScalaUDF(_, _, _, _) => true
            case _                    => false
          }
          if(st_contains.nonEmpty) {
            // we got an st_contains, extract the geometry and set up the new GeoMesa relation with the appropriate
            // CQL filter

            // TODO: only dealing with one st_contains at the moment
            //            val ScalaUDF(func, _, Seq(GeometryLiteral(_, geom), a), _) = st_contains.head
            val ScalaUDF(func, _, Seq(exprA, exprB), _) = st_contains.head

            // TODO: map func => ff.function
            // TODO: Map Expressions to OpenGIS expressions.

            val builder: (GTExpression, GTExpression) => org.opengis.filter.Filter = funcToFF(func)
            val gtExprA = sparkExprToGTExpr(exprA)
            val gtExprB = sparkExprToGTExpr(exprB)

            val cqlFilter = builder(gtExprA, gtExprB)

            val relation = gmRel.copy(filt = ff.and(gmRel.filt, cqlFilter))
            // need to maintain expectedOutputAttributes so identifiers don't change in projections
            val newrel = lr.copy(expectedOutputAttributes = Some(lr.output), relation = relation)
            if(xs.nonEmpty) {
              // if there are other filters, keep them
              Filter(xs.reduce(And), newrel)
            } else {
              // if st_contains was the only filter, just return the new relation
              newrel
            }
          } else {
            filt
          }
      }
    }

    def funcToFF(func: AnyRef) = {
      func match {
        case ST_Contains => (expr1: GTExpression, expr2: GTExpression) =>
          ff.contains(expr1, expr2)
        case ST_Crosses => (expr1: GTExpression, expr2: GTExpression) =>
          ff.crosses(expr1, expr2)
        case ST_Disjoint => (expr1: GTExpression, expr2: GTExpression) =>
          ff.disjoint(expr1, expr2)
        case ST_Equals => (expr1: GTExpression, expr2: GTExpression) =>
          ff.equal(expr1, expr2)
        case ST_Intersects => (expr1: GTExpression, expr2: GTExpression) =>
          ff.intersects(expr1, expr2)
        case ST_Overlaps => (expr1: GTExpression, expr2: GTExpression) =>
          ff.overlaps(expr1, expr2)
        case ST_Touches => (expr1: GTExpression, expr2: GTExpression) =>
          ff.touches(expr1, expr2)
        case ST_Within => (expr1: GTExpression, expr2: GTExpression) =>
          ff.within(expr1, expr2)
      }
    }

    def sparkExprToGTExpr(expr: org.apache.spark.sql.catalyst.expressions.Expression): org.opengis.filter.expression.Expression = {
      expr match {
        case GeometryLiteral(_, geom) =>
          ff.literal(geom)
        case AttributeReference(name, _, _, _) =>
          ff.property(name)
        case _ =>
          log.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
          ff.property("geom")
      }
    }
  }

  import SQLSpatialFunctions._

  object FoldConstantGeometryRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case ScalaUDF(ST_GeomFromWKT, GeometryTypeInstance, Seq(Literal(wkt, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
            val geom = ST_GeomFromWKT(wkt.asInstanceOf[UTF8String].toString)
            GeometryLiteral(GeometryUDT.serialize(geom), geom)
        }
      }
    }
  }

  def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(FoldConstantGeometryRule, STContainsRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }

    Seq.empty[Strategy].foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }
}
