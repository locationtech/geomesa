/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.expression

import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, Literal}


object FastComparisonOperator {

  def lessThan(exp1: Expression, exp2: Literal): PropertyIsLessThan = new FastLessThanLiteral(exp1, exp2)
  def lessThan(exp1: Literal, exp2: Expression): PropertyIsLessThan = new FastLessThanExpression(exp1, exp2)

  def lessThanOrEqual(exp1: Expression, exp2: Literal): PropertyIsLessThanOrEqualTo =
    new FastLessThanOrEqualToLiteral(exp1, exp2)
  def lessThanOrEqual(exp1: Literal, exp2: Expression): PropertyIsLessThanOrEqualTo =
    new FastLessThanOrEqualToExpression(exp1, exp2)

  def greaterThan(exp1: Expression, exp2: Literal): PropertyIsGreaterThan = new FastGreaterThanLiteral(exp1, exp2)
  def greaterThan(exp1: Literal, exp2: Expression): PropertyIsGreaterThan = new FastGreaterThanExpression(exp1, exp2)

  def greaterThanOrEqual(exp1: Expression, exp2: Literal): PropertyIsGreaterThanOrEqualTo =
    new FastGreaterThanOrEqualToLiteral(exp1, exp2)
  def greaterThanOrEqual(exp1: Literal, exp2: Expression): PropertyIsGreaterThanOrEqualTo =
    new FastGreaterThanOrEqualToExpression(exp1, exp2)

  /**
    * Less than filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastLessThanLiteral(exp1: Expression, exp2: Literal)
      extends FastComparisonOperator(exp1, exp2, "<") with PropertyIsLessThan {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp1.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && value.compareTo(lit) < 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Less than filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastLessThanExpression(exp1: Literal, exp2: Expression)
      extends FastComparisonOperator(exp1, exp2, "<") with PropertyIsLessThan {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp2.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && lit.compareTo(value) < 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Less than filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastLessThanOrEqualToLiteral(exp1: Expression, exp2: Literal)
      extends FastComparisonOperator(exp1, exp2, "<=") with PropertyIsLessThanOrEqualTo {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp1.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && value.compareTo(lit) <= 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Less than filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastLessThanOrEqualToExpression(exp1: Literal, exp2: Expression)
      extends FastComparisonOperator(exp1, exp2, "<=") with PropertyIsLessThanOrEqualTo {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp2.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && lit.compareTo(value) <= 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Greater than filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastGreaterThanLiteral(exp1: Expression, exp2: Literal)
      extends FastComparisonOperator(exp1, exp2, ">") with PropertyIsGreaterThan {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp1.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && value.compareTo(lit) > 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Greater than filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastGreaterThanExpression(exp1: Literal, exp2: Expression)
      extends FastComparisonOperator(exp1, exp2, ">") with PropertyIsGreaterThan {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp2.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && lit.compareTo(value) > 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Greater than or equal filter comparing an expression (e.g. property or function) to a literal
    *
    * @param exp1 expression
    * @param exp2 literal
    */
  private final class FastGreaterThanOrEqualToLiteral(exp1: Expression, exp2: Literal)
      extends FastComparisonOperator(exp1, exp2, ">=") with PropertyIsGreaterThanOrEqualTo {

    private val lit = FastConverter.convert(exp2.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp1.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && value.compareTo(lit) >= 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }

  /**
    * Greater than or equal filter comparing a literal to an expression (e.g. property or function)
    *
    * @param exp1 literal
    * @param exp2 expression
    */
  private final class FastGreaterThanOrEqualToExpression(exp1: Literal, exp2: Expression)
      extends FastComparisonOperator(exp1, exp2, ">=") with PropertyIsGreaterThanOrEqualTo {

    private val lit = FastConverter.convert(exp1.evaluate(null), classOf[Comparable[Any]])

    override def evaluate(obj: AnyRef): Boolean = {
      val value = exp2.evaluate(obj).asInstanceOf[Comparable[Any]]
      value != null && lit != null && lit.compareTo(value) >= 0
    }

    override def accept(visitor: FilterVisitor, extraData: AnyRef): AnyRef = visitor.visit(this, extraData)
  }
}

sealed private abstract class FastComparisonOperator(exp1: Expression, exp2: Expression, private val op: String)
    extends BinaryComparisonOperator {

  override def getExpression1: Expression = exp1

  override def getExpression2: Expression = exp2

  override def isMatchingCase: Boolean = false

  override def getMatchAction: MultiValuedFilter.MatchAction = MultiValuedFilter.MatchAction.ANY

  override def toString: String = s"[ $exp1 $op $exp2 ]"

  def canEqual(other: Any): Boolean = other.isInstanceOf[FastComparisonOperator]

  override def equals(other: Any): Boolean = other match {
    case that: FastComparisonOperator =>
      (that canEqual this) && exp1 == that.getExpression1 && exp2 == that.getExpression2 && op == that.op
    case _ => false
  }

  override def hashCode(): Int =
    Seq(exp1, exp2, op).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}
