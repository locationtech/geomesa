/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.jts.JTSTypes._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, LeafExpression,  PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, StructType}
import org.locationtech.geomesa.spark.{GeoMesaJoinRelation, GeoMesaRelation, RelationUtils}
import org.opengis.filter.expression.{Expression => GTExpression}
import org.opengis.filter.{Filter => GTFilter}

import scala.collection.JavaConversions._
import scala.util.Try

object SQLRules extends LazyLogging {
  import org.locationtech.geomesa.spark.jts.udf.SQLSpatialFunctions._

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
      case _ =>
        logger.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
        None
    }
  }

  // new AST expressions
  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryTypeInstance
  }

  // new optimizations rules
  object STContainsRule extends Rule[LogicalPlan] with PredicateHelper {


    // JNH: NB: Unused.
    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
      case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
      case ScalaUDF(_, _, Seq(_, GeometryLiteral(_, geom)), _, _) => Some(geom)
      case _ => None
    }

    private def extractScalaUDFs(f: Expression) = {
      splitConjunctivePredicates(f).partition {
        // TODO: Add guard which checks to see if the function can be pushed down
        case ScalaUDF(_, _, _, _, _) => true
        case _ => false
      }
    }

    private def extractGridId(envelopes: List[Envelope], e: org.apache.spark.sql.catalyst.expressions.Expression): Option[List[Int]] = e match {
      case And(l, r) => Some(extractGridId(envelopes, l).getOrElse(List()) ++  extractGridId(envelopes, r).getOrElse(List()))
      case ScalaUDF(_, _, Seq(_, GeometryLiteral(_, geom)), _, _) => Some(RelationUtils.gridIdMapper(geom, envelopes))
      case GeometryLiteral(_,geom) => Some(RelationUtils.gridIdMapper(geom, envelopes))
      case _ => None
    }

    // Converts a pair of GeoMesaRelations into one GeoMesaJoinRelation
    private def alterRelation(left: GeoMesaRelation, right: GeoMesaRelation, cond: Expression): GeoMesaJoinRelation = {
      val joinedSchema = StructType(left.schema.fields ++ right.schema.fields)
      GeoMesaJoinRelation(left.sqlContext, left, right, joinedSchema, cond)
    }

    // Replace the relation in a join with a GeoMesaJoin Relation
    private def alterJoin(join: Join): LogicalPlan = {
      join match {
        case Join(leftLr@LogicalRelation(leftRel: GeoMesaRelation, _, _),
                  rightLr@LogicalRelation(rightRel: GeoMesaRelation, _, _),
                  joinType,
                  condition) =>
          val isSpatialUDF = condition.get match {
            case ScalaUDF(function: ((Geometry, Geometry) => java.lang.Boolean), _, children, _, _) =>
              children(0).isInstanceOf[AttributeReference] && children(1).isInstanceOf[AttributeReference]
            case _ => false
          }
          if (isSpatialUDF && leftRel.spatiallyPartition && rightRel.spatiallyPartition) {
            if (leftRel.partitionEnvelopes != rightRel.partitionEnvelopes) {
              logger.warn("Joining across two relations that are not partitioned by the same scheme. Unable to optimize")
              join
            } else {
              val joinRelation = alterRelation(leftRel, rightRel, condition.get)
              val newLogicalRelLeft = leftLr.copy(output = leftLr.output ++ rightLr.output, relation = joinRelation)
              Join(newLogicalRelLeft, rightLr, joinType, condition)
            }
          } else {
            join
          }
        case Join(leftProject@Project(leftProjectList,leftLr@LogicalRelation(leftRel: GeoMesaRelation, _, _)),
                  rightProject@Project(rightProjectList,rightLr@LogicalRelation(rightRel: GeoMesaRelation, _, _)),
                  joinType,
                  condition) =>
          val isSpatialUDF = condition.get match {
            case ScalaUDF(function: ((Geometry, Geometry) => java.lang.Boolean), _, children, _, _) =>
              children(0).isInstanceOf[AttributeReference] && children(1).isInstanceOf[AttributeReference]
            case _ => false
          }
          if (isSpatialUDF && leftRel.spatiallyPartition && rightRel.spatiallyPartition) {
            if (leftRel.partitionEnvelopes != rightRel.partitionEnvelopes) {
              if (leftRel.coverPartition) {
                rightRel.partitionEnvelopes = leftRel.partitionEnvelopes
                rightRel.partitionedRDD = RelationUtils.spatiallyPartition(leftRel.partitionEnvelopes,
                                                                           rightRel.rawRDD,
                                                                           leftRel.numPartitions,
                                                                           rightRel.geometryOrdinal)
                val joinRelation = alterRelation(leftRel, rightRel, condition.get)
                val newLogicalRelLeft = leftLr.copy(output = leftLr.output ++ rightLr.output, relation = joinRelation)
                val newProjectLeft = leftProject.copy(projectList = leftProjectList ++ rightProjectList, child = newLogicalRelLeft)
                Join(newProjectLeft, rightProject, joinType, condition)
              } else {
                logger.warn("Joining across two relations that are not partitioned by the same scheme. Unable to optimize")
                join
              }
            } else {
              val joinRelation = alterRelation(leftRel, rightRel, condition.get)
              val newLogicalRelLeft = leftLr.copy(output = leftLr.output ++ rightLr.output, relation = joinRelation)
              val newProjectLeft = leftProject.copy(projectList = leftProjectList ++ rightProjectList, child = newLogicalRelLeft)
              Join(newProjectLeft, rightProject, joinType, condition)
            }
          } else {
            join
          }
        case _ => join
      }
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      logger.debug(s"Optimizer sees $plan")
      plan.transform {
        case agg @ Aggregate(aggregateExpressions,groupingExpressions, Project(projectList, join: Join)) =>
          val alteredJoin = alterJoin(join)
          Aggregate(aggregateExpressions, groupingExpressions, Project(projectList, alteredJoin))
        case agg @ Aggregate(aggregateExpressions,groupingExpressions, join: Join) =>
          val alteredJoin = alterJoin(join)
          Aggregate(aggregateExpressions, groupingExpressions, alteredJoin)
        case join: Join =>
          alterJoin(join)
        case sort @ Sort(_, _, _) => sort    // No-op.  Just realizing what we can do:)
        case filt @ Filter(f, lr@LogicalRelation(gmRel: GeoMesaRelation, _, _)) =>
          // TODO: deal with `or`

          // split up conjunctive predicates and extract the st_contains variable
          val (scalaUDFs: Seq[Expression], otherFilters: Seq[Expression]) = extractScalaUDFs(f)

          val (gtFilters: Seq[GTFilter], sFilters: Seq[Expression]) = scalaUDFs.foldLeft((Seq[GTFilter](), otherFilters)) {
            case ((gts: Seq[GTFilter], sfilters), expression: Expression) =>
              val cqlFilter = scalaUDFtoGTFilter(expression)

              cqlFilter match {
                case Some(gtf) => (gts.+:(gtf), sfilters)
                case None      => (gts,         sfilters.+:(expression))
              }
          }

          val partitionHints = if (gmRel.spatiallyPartition) {
            val hints = scalaUDFs.flatMap{e => extractGridId(gmRel.partitionEnvelopes, e) }.flatten
            if (hints.nonEmpty) {
              hints
            } else {
              null
            }
          } else {
            null
          }

          if (gtFilters.nonEmpty) {
            val relation = gmRel.copy(filt = ff.and(gtFilters :+ gmRel.filt), partitionHints = partitionHints)
            val newrel = lr.copy(output = lr.output, relation = relation)
            if (sFilters.nonEmpty) {
              Filter(sFilters.reduce(And), newrel)
            } else {
              // if st_contains was the only filter, just return the new relation
              newrel
            }
          } else {
            filt
          }
      }
    }

  }

  // A catch for when we are able to precompute the join using the sweepline algorithm.
  // Skips doing a full cartesian product with catalyst.
  object SpatialJoinStrategy extends Strategy {

    import org.apache.spark.sql.catalyst.plans.logical._

    def alterJoin(logicalPlan: Join): Seq[SparkPlan] = {
      logicalPlan.left match {
        case Project(projectList, lr@LogicalRelation(gmRel: GeoMesaJoinRelation, _, _)) =>
          ProjectExec(projectList, planLater(lr)) :: Nil
        case lr@LogicalRelation(gmRel: GeoMesaJoinRelation, _, _) =>
          planLater(lr) :: Nil
        case _ => Nil
      }
    }

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      //TODO: handle other kinds of joins
      case Project(_, logicalPlan: Join) =>
        alterJoin(logicalPlan)
      case join: Join =>
        alterJoin(join)
      case _ => Nil
    }
  }

  def registerOptimizations(sqlContext: SQLContext): Unit = {

    Seq(SpatialJoinStrategy).foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }
}
