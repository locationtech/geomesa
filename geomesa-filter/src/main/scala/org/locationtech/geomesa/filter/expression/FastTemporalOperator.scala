/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import java.util.Date

import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.filter.FilterVisitor
import org.opengis.filter.MultiValuedFilter.MatchAction
import org.opengis.filter.expression.{Expression, Literal}
import org.opengis.filter.temporal.{After, Before, BinaryTemporalOperator, During}
import org.opengis.temporal.Period

/**
  * Fast temporal filters that avoid repeatedly evaluating literals
  */
object FastTemporalOperator {

  def after(exp1: Expression, exp2: Literal): After = new FastAfterLiteral(exp1, exp2)
  def after(exp1: Literal, exp2: Expression): After = new FastAfterExpression(exp1, exp2)

  def before(exp1: Expression, exp2: Literal): Before = new FastBeforeLiteral(exp1, exp2)
  def before(exp1: Literal, exp2: Expression): Before = new FastBeforeExpression(exp1, exp2)

  def during(exp1: Expression, exp2: Literal): During = new FastDuring(exp1, exp2)

  /**
    * After filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastAfterLiteral(exp1: Expression, exp2: Literal)
      extends FastTemporalOperator(exp1, exp2, After.NAME) with After {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Date])

    override def evaluate(obj: AnyRef): Boolean = {
      val date = exp1.evaluate(obj).asInstanceOf[Date]
      date != null && date.after(lit)
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * After filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastAfterExpression(exp1: Literal, exp2: Expression)
      extends FastTemporalOperator(exp1, exp2, After.NAME) with After {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Date])

    override def evaluate(obj: AnyRef): Boolean = {
      val date = exp2.evaluate(obj).asInstanceOf[Date]
      date != null && lit.after(date)
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Before filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastBeforeLiteral(exp1: Expression, exp2: Literal)
      extends FastTemporalOperator(exp1, exp2, Before.NAME) with Before {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Date])

    override def evaluate(obj: AnyRef): Boolean = {
      val date = exp1.evaluate(obj).asInstanceOf[Date]
      date != null && date.before(lit)
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Before filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastBeforeExpression(exp1: Literal, exp2: Expression) extends
      FastTemporalOperator(exp1, exp2, Before.NAME) with Before {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Date])

    override def evaluate(obj: AnyRef): Boolean = {
      val date = exp1.evaluate(obj).asInstanceOf[Date]
      date != null && lit.before(date)
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }


  /**
    * During filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastDuring(exp1: Expression, exp2: Literal)
      extends FastTemporalOperator(exp1, exp2, During.NAME) with During {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Period])
    private val beg = lit.getBeginning.getPosition.getDate
    private val end = lit.getEnding.getPosition.getDate

    override def evaluate(obj: AnyRef): Boolean = {
      val date = exp1.evaluate(obj).asInstanceOf[Date]
      date != null && date.after(beg) && date.before(end)
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }
}

sealed private abstract class FastTemporalOperator(exp1: Expression, exp2: Expression, private val op: String)
    extends BinaryTemporalOperator {

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def getMatchAction: MatchAction = MatchAction.ANY

  override def toString: String = s"[ $exp1 $op $exp2 ]"

  def canEqual(other: Any): Boolean = other.isInstanceOf[FastTemporalOperator]

  override def equals(other: Any): Boolean = other match {
    case that: FastTemporalOperator =>
      (that canEqual this) && exp1 == that.getExpression1 && exp2 == that.getExpression2 && op == that.op
    case _ => false
  }

  override def hashCode(): Int =
    Seq(exp1, exp2, op).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}
