/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
<<<<<<< HEAD
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT => Sedona_GeometryUDT}
import org.apache.spark.sql.sedona_sql.expressions.{ST_Contains => Sedona_ST_Contains, ST_Crosses => Sedona_ST_Crosses, ST_Equals => Sedona_ST_Equals, ST_Intersects => Sedona_ST_Intersects, ST_Overlaps => Sedona_ST_Overlaps, ST_Predicate => Sedona_ST_Predicate, ST_Touches => Sedona_ST_Touches, ST_Within => Sedona_ST_Within}
import org.apache.spark.sql.types.DataTypes
=======
<<<<<<< HEAD
import org.apache.spark.sql.sedona_sql.expressions.{ST_Contains => Sedona_ST_Contains, ST_Crosses => Sedona_ST_Crosses, ST_Equals => Sedona_ST_Equals, ST_Intersects => Sedona_ST_Intersects, ST_Overlaps => Sedona_ST_Overlaps, ST_Predicate => Sedona_ST_Predicate, ST_Touches => Sedona_ST_Touches, ST_Within => Sedona_ST_Within}
import org.apache.spark.sql.sedona_sql.UDT.{GeometryUDT => Sedona_GeometryUDT}
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
import org.apache.spark.sql.types.{DataTypes, StructType}
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
import org.apache.spark.sql.{SQLContext, Strategy}
import org.geotools.api.filter.expression.{Expression => GTExpression, Literal => GTLiteral}
import org.geotools.api.filter.{FilterFactory, Filter => GTFilter}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.spark.haveSedona
import org.locationtech.geomesa.spark.jts.rules.GeometryLiteral
import org.locationtech.geomesa.spark.jts.udf.SpatialRelationFunctions._
<<<<<<< HEAD
=======
<<<<<<< HEAD
import org.locationtech.geomesa.spark.haveSedona
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb0bd279638 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4a6d96f2b4e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 603c7b9204a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> be6b3b14b4a (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 4a6d96f2b4e (GEOMESA-3254 Add Bloop build support)
import org.locationtech.geomesa.spark.sql.GeoMesaRelation.PartitionedIndexedRDD
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.jts.geom.{Envelope, Geometry}

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Date

object SQLRules extends LazyLogging {
  @transient
  private val ff: FilterFactory = CommonFactoryFinder.getFilterFactory

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

<<<<<<< HEAD
  def sedonaExprToGTFilter(pred: Sedona_ST_Predicate): Option[GTFilter] = {
    val left = pred.children.head
    val right = pred.children.last
    (sparkExprToGTExpr(left), sparkExprToGTExpr(right)) match {
      case (Some(expr1), Some(expr2)) => pred match {
        case Sedona_ST_Contains(_) => Some(ff.contains(expr1, expr2))
        case Sedona_ST_Crosses(_) => Some(ff.crosses(expr1, expr2))
        case Sedona_ST_Overlaps(_) => Some(ff.overlaps(expr1, expr2))
        case Sedona_ST_Intersects(_) => Some(ff.intersects(expr1, expr2))
        case Sedona_ST_Within(_) => Some(ff.within(expr1, expr2))
        case Sedona_ST_Touches(_) => Some(ff.touches(expr1, expr2))
        case Sedona_ST_Equals(_) => Some(ff.equal(expr1, expr2))
        case _ => None
      }
      case _ => None
    }
  }

=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
        if (haveSedona && expr.isInstanceOf[Sedona_ST_Predicate]) {
          sedonaExprToGTFilter(expr.asInstanceOf[Sedona_ST_Predicate])
        } else {
          logger.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
          None
        }
=======
        logger.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
        None
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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

<<<<<<< HEAD
    case lit: Literal if haveSedona && lit.dataType.isInstanceOf[Sedona_GeometryUDT] =>
      Some(ff.literal(lit.dataType.asInstanceOf[Sedona_GeometryUDT].deserialize(lit.value)))

=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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

<<<<<<< HEAD
=======
    // Converts a pair of GeoMesaRelations into one GeoMesaJoinRelation
    private def alterRelation(left: GeoMesaRelation, right: GeoMesaRelation, cond: Expression): GeoMesaJoinRelation = {
      val joinedSchema = StructType(left.schema.fields ++ right.schema.fields)
      GeoMesaJoinRelation(left.sqlContext, left, right, joinedSchema, cond)
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
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
            case (leftRel: GeoMesaRelation, rightRel: GeoMesaRelation) =>
              leftRel.join(rightRel, join.condition.get) match {
                case None => join
                case Some(joinRelation) =>
                  val newLogicalRelLeft = SparkVersions.copy(left)(output = left.output ++ right.output, relation = joinRelation)
                  SparkVersions.copy(join)(left = newLogicalRelLeft)
              }

            case _ => join
          }

        case (leftProject @ Project(leftProjectList, left: LogicalRelation),
<<<<<<< HEAD
            Project(rightProjectList, right: LogicalRelation)) if isSpatialUDF =>
=======
            rightProject @ Project(rightProjectList, right: LogicalRelation)) if isSpatialUDF =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
          (left.relation, right.relation) match {
            case (leftRel: GeoMesaRelation, rightRel: GeoMesaRelation) =>
              leftRel.join(rightRel, join.condition.get) match {
                case None => join
                case Some(joinRelation) =>
                  val newLogicalRelLeft = SparkVersions.copy(left)(output = left.output ++ right.output, relation = joinRelation)
                  val newProjectLeft = leftProject.copy(projectList = leftProjectList ++ rightProjectList, child = newLogicalRelLeft)
                  SparkVersions.copy(join)(left = newProjectLeft)
              }

            case _ => join
          }

        case _ => join
      }
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      logger.debug(s"Optimizer sees $plan")

      // NOTE: The number of arguments in Aggregate constructor is 3 on community spark and 4 on DataBricks
      // so we cannot use pattern matching to unapply the constructor also
      // need to use reflection to safely create new Aggregate instance
      val optimizeAggregate: PartialFunction[LogicalPlan, LogicalPlan] =
        Function.unlift { plan: LogicalPlan =>
          plan match {
            case agg: Aggregate =>
              agg.child match {
                case Project(projectList, join: Join) =>
                  val alteredJoin = SpatialOptimizationsRule.alterJoin(join)
                  Some(Aggregates.instance(agg.groupingExpressions, agg.aggregateExpressions, Project(projectList, alteredJoin), None))
                case join: Join =>
                  val alteredJoin = SpatialOptimizationsRule.alterJoin(join)
                  Some(Aggregates.instance(agg.groupingExpressions, agg.aggregateExpressions, alteredJoin, None))
                case _ => None
              }
            case _ => None
          }
        }

      val optimizeRest: PartialFunction[LogicalPlan, LogicalPlan] = {
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
              sparkFilterToGTFilter(expression) match {
                case Some(gtf) => (gts.+:(gtf), sfilters)
                case None      => (gts,         sfilters.+:(expression))
              }
          }

          if (gtFilters.nonEmpty) {
            // if we have a partitioned cache, exclude partitions that don't match the query filter
            val partitioned = gmRel.cached.map {
              case c: PartitionedIndexedRDD =>
                val hints = sparkFilters.flatMap(extractGridId(c.envelopes, _)).flatten
                if (hints.isEmpty) { c } else {
                  c.copy(rdd = c.rdd.filter { case (key, _) => hints.contains(key) })
                }

              case c => c
            }
            val filt = FilterHelper.filterListAsAnd(gmRel.filter.toSeq ++ gtFilters)
            val relation = gmRel.copy(filter = filt, cached = partitioned)
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
      plan.transform(optimizeAggregate orElse optimizeRest)
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
