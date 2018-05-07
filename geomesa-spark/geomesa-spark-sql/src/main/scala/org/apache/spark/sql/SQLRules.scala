/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral
import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._
import org.locationtech.geomesa.spark.{GeoMesaJoinRelation, GeoMesaRelation, RelationUtils}
import org.opengis.filter.expression.{Expression => GTExpression, Literal => GTLiteral}
import org.opengis.filter.{FilterFactory2, Filter => GTFilter}

import scala.collection.JavaConversions._

object SQLRules extends LazyLogging {
  @transient
  private val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

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

  def sparkFilterToGTFilter(expr: Expression): Option[GTFilter] = {
    expr match {
      case udf: ScalaUDF => scalaUDFtoGTFilter(udf)
      case binaryComp@BinaryComparison(left, right) =>
        val leftExpr = sparkExprToGTExpr(left)
        val rightExpr = sparkExprToGTExpr(right)
        if (leftExpr.isEmpty || rightExpr.isEmpty) {
          None
        } else {
          binaryComp match {
            case eq:  EqualTo => Some(ff.equals(leftExpr.get, rightExpr.get))
            case lt:  LessThan => Some(ff.less(leftExpr.get, rightExpr.get))
            case lte: LessThanOrEqual => Some(ff.lessOrEqual(leftExpr.get, rightExpr.get))
            case gt:  GreaterThan => Some(ff.greater(leftExpr.get, rightExpr.get))
            case gte: GreaterThanOrEqual => Some(ff.greaterOrEqual(leftExpr.get, rightExpr.get))
            case _ => None
          }
        }
      case unary: UnaryExpression =>
        val sparkExpr = unary.child
        val gtExpr = sparkExprToGTExpr(sparkExpr)
        if (gtExpr.isEmpty)
          None
        else {
          unary match {
            case _: IsNotNull => Some(ff.not(ff.isNull(gtExpr.get)))
            case _: IsNull => Some(ff.isNull(gtExpr.get))
            case _ => None
          }
        }
      case _ =>
        logger.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
        None
    }
  }

  def sparkExprToGTExpr(expression: Expression): Option[GTExpression] = expression match {
    case g: GeometryLiteral =>
      Some(ff.literal(g.geom))

    case a: AttributeReference if a.name != "__fid__" =>
      Some(ff.property(a.name))

    case c: Cast =>
      lazy val zone = c.timeZoneId.map(ZoneId.of).orNull
      sparkExprToGTExpr(c.child).map {
        case lit: GTLiteral if lit.getValue.isInstanceOf[Date] && zone != null =>
          val date = LocalDateTime.ofInstant(lit.getValue.asInstanceOf[Date].toInstant, zone)
          ff.literal(new Date(date.atZone(ZoneOffset.UTC).toInstant.toEpochMilli))
        case e => e
      }

    case lit: Literal if lit.dataType == DataTypes.StringType =>
      // the actual class is org.apache.spark.unsafe.types.UTF8String, we need to make it
      // a normal string so that geotools can handle it
      Some(ff.literal(Option(lit.value).map(_.toString).orNull))

    case lit: Literal if lit.dataType == DataTypes.TimestampType =>
      // timestamps are defined as microseconds
      Some(ff.literal(new Date(lit.value.asInstanceOf[Long] / 1000)))

    case lit: Literal =>
      Some(ff.literal(lit.value))

    case _ =>
      logger.debug(s"Can't turn expression into geotools: $expression")
      None
  }

  // new optimizations rules
  object SpatialOptimizationsRule extends Rule[LogicalPlan] with PredicateHelper {


    // JNH: NB: Unused.
    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
      case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
      case ScalaUDF(_, _, Seq(_, GeometryLiteral(_, geom)), _, _) => Some(geom)
      case _ => None
    }

    private def extractGridId(envelopes: List[Envelope], e: org.apache.spark.sql.catalyst.expressions.Expression): Option[List[Int]] = e match {
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
              children.head.isInstanceOf[AttributeReference] && children(1).isInstanceOf[AttributeReference]
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
              children.head.isInstanceOf[AttributeReference] && children(1).isInstanceOf[AttributeReference]
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
          val sparkFilters: Seq[Expression] =  splitConjunctivePredicates(f)

          val (gtFilters: Seq[GTFilter], sFilters: Seq[Expression]) = sparkFilters.foldLeft((Seq[GTFilter](), Seq[Expression]())) {
            case ((gts: Seq[GTFilter], sfilters), expression: Expression) =>
              val cqlFilter = sparkFilterToGTFilter(expression)

              cqlFilter match {
                case Some(gtf) => (gts.+:(gtf), sfilters)
                case None      => (gts,         sfilters.+:(expression))
              }
          }

          val partitionHints = if (gmRel.spatiallyPartition) {
            val hints = sparkFilters.flatMap{e => extractGridId(gmRel.partitionEnvelopes, e) }.flatten
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
              // Keep filters that couldn't be transformed at the top level
              Filter(sFilters.reduce(And), newrel)
            } else {
              // if all filters could be transformed to GeoTools filters, just return the new relation
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

    Seq(SpatialOptimizationsRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }

    Seq(SpatialJoinStrategy).foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }
}
