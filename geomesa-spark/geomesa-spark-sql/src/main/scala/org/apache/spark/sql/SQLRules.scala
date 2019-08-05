/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral
import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._
import org.locationtech.geomesa.spark.{GeoMesaJoinRelation, GeoMesaRelation, RelationUtils, SparkVersions}
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.filter.expression.{Expression => GTExpression, Literal => GTLiteral}
import org.opengis.filter.{FilterFactory2, Filter => GTFilter}

import scala.collection.JavaConversions._

object SQLRules extends LazyLogging {
  @transient
  private val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

  def scalaUDFtoGTFilter(udf: Expression): Option[GTFilter] = {
    udf match {
      case u: ScalaUDF if u.children.length == 2 => buildGTFilter(u.function, u.children.head, u.children.last)
      case _ => None
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
            case _: EqualTo            => Some(ff.equals(leftExpr.get, rightExpr.get))
            case _: LessThan           => Some(ff.less(leftExpr.get, rightExpr.get))
            case _: LessThanOrEqual    => Some(ff.lessOrEqual(leftExpr.get, rightExpr.get))
            case _: GreaterThan        => Some(ff.greater(leftExpr.get, rightExpr.get))
            case _: GreaterThanOrEqual => Some(ff.greaterOrEqual(leftExpr.get, rightExpr.get))
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
          val date = LocalDateTime.ofInstant(toInstant(lit.getValue.asInstanceOf[Date]), zone)
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

    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
      case GeometryLiteral(_, geom) => Some(geom)
      case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
      case u: ScalaUDF => u.children.collectFirst { case GeometryLiteral(_, geom) => geom }
      case _ => None
    }

    private def extractGridId(envelopes: List[Envelope],
                              e: org.apache.spark.sql.catalyst.expressions.Expression): Option[List[Int]] =
      extractGeometry(e).map(RelationUtils.gridIdMapper(_, envelopes))

    // Converts a pair of GeoMesaRelations into one GeoMesaJoinRelation
    private def alterRelation(left: GeoMesaRelation, right: GeoMesaRelation, cond: Expression): GeoMesaJoinRelation = {
      val joinedSchema = StructType(left.schema.fields ++ right.schema.fields)
      GeoMesaJoinRelation(left.sqlContext, left, right, joinedSchema, cond)
    }

    // Replace the relation in a join with a GeoMesaJoin Relation
    private def alterJoin(join: Join): LogicalPlan = {
      val isSpatialUDF = join.condition.exists {
        case u: ScalaUDF if u.function.isInstanceOf[(Geometry, Geometry) => java.lang.Boolean] =>
          u.children.head.isInstanceOf[AttributeReference] && u.children(1).isInstanceOf[AttributeReference]
        case _ => false
      }

      (join.left, join.right) match {
        case (left: LogicalRelation, right: LogicalRelation) if isSpatialUDF =>
          (left.relation, right.relation) match {
            case (leftRel: GeoMesaRelation, rightRel: GeoMesaRelation) if leftRel.spatiallyPartition && rightRel.spatiallyPartition =>
              if (leftRel.partitionEnvelopes != rightRel.partitionEnvelopes) {
                logger.warn("Joining across two relations that are not partitioned by the same scheme - " +
                    "unable to optimize")
                join
              } else {
                val joinRelation = alterRelation(leftRel, rightRel, join.condition.get)
                val newLogicalRelLeft = SparkVersions.copy(left)(output = left.output ++ right.output, relation = joinRelation)
                Join(newLogicalRelLeft, right, join.joinType, join.condition)
              }

            case _ => join
          }

        case (leftProject @ Project(leftProjectList, left: LogicalRelation),
            rightProject @ Project(rightProjectList, right: LogicalRelation)) if isSpatialUDF =>
          (left.relation, right.relation) match {
            case (leftRel: GeoMesaRelation, rightRel: GeoMesaRelation) if leftRel.spatiallyPartition && rightRel.spatiallyPartition =>
              if (leftRel.partitionEnvelopes == rightRel.partitionEnvelopes) {
                val joinRelation = alterRelation(leftRel, rightRel, join.condition.get)
                val newLogicalRelLeft = SparkVersions.copy(left)(output = left.output ++ right.output, relation = joinRelation)
                val newProjectLeft = leftProject.copy(projectList = leftProjectList ++ rightProjectList, child = newLogicalRelLeft)
                Join(newProjectLeft, rightProject, join.joinType, join.condition)
              } else if (leftRel.coverPartition) {
                rightRel.partitionEnvelopes = leftRel.partitionEnvelopes
                rightRel.partitionedRDD = RelationUtils.spatiallyPartition(leftRel.partitionEnvelopes,
                  rightRel.rawRDD,
                  leftRel.numPartitions,
                  rightRel.geometryOrdinal)
                val joinRelation = alterRelation(leftRel, rightRel, join.condition.get)
                val newLogicalRelLeft = SparkVersions.copy(left)(output = left.output ++ right.output, relation = joinRelation)
                val newProjectLeft = leftProject.copy(projectList = leftProjectList ++ rightProjectList, child = newLogicalRelLeft)
                Join(newProjectLeft, rightProject, join.joinType, join.condition)
              } else {
                logger.warn("Joining across two relations that are not partitioned by the same scheme - " +
                    "unable to optimize")
                join
              }

            case _ => join
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
        case filt @ Filter(f, lr: LogicalRelation) if lr.relation.isInstanceOf[GeoMesaRelation] =>
          // TODO: deal with `or`

          val gmRel = lr.relation.asInstanceOf[GeoMesaRelation]
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
            val newrel = SparkVersions.copy(lr)(output = lr.output, relation = relation)
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
        case Project(projectList, lr: LogicalRelation) if lr.relation.isInstanceOf[GeoMesaJoinRelation] =>
           ProjectExec(projectList, planLater(lr)) :: Nil

        case lr: LogicalRelation if lr.relation.isInstanceOf[GeoMesaJoinRelation] => planLater(lr) :: Nil

        case _ => Nil
      }
    }

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      //TODO: handle other kinds of joins
      case Project(_, logicalPlan: Join) => alterJoin(logicalPlan)
      case join: Join => alterJoin(join)
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
