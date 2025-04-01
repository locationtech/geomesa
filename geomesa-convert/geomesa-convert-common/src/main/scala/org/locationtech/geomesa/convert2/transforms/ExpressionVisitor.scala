/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert2.transforms.Expression._

/**
 * Visitor pattern for manipulating expressions
 *
 * @tparam T result type
 */
trait ExpressionVisitor[T] {
  def visit[U <: AnyRef](e: Literal[U]): T
  def visit(e: CastToInt): T
  def visit(e: CastToLong): T
  def visit(e: CastToFloat): T
  def visit(e: CastToDouble): T
  def visit(e: CastToBoolean): T
  def visit(e: CastToString): T
  def visit(e: FunctionExpression): T
  def visit(e: TryExpression): T
  def visit(e: WithDefaultExpression): T
  def visit(e: FieldLookup): T
  def visit(e: Column): T
  def visit(e: RegexExpression): T
}

object ExpressionVisitor {

  /**
   * Base class to manipulate expressions by visiting every expression in the tree
   */
  class ExpressionTreeVisitor extends ExpressionVisitor[Expression] {
    override def visit[U <: AnyRef](e: Literal[U]): Expression = e
    override def visit(e: Column): Expression = e
    override def visit(e: RegexExpression): Expression = e
    override def visit(e: FieldLookup): Expression = e
    override def visit(e: CastToInt): Expression = e.copy(e.e.accept(this))
    override def visit(e: CastToLong): Expression = e.copy(e.e.accept(this))
    override def visit(e: CastToFloat): Expression = e.copy(e.e.accept(this))
    override def visit(e: CastToDouble): Expression = e.copy(e.e.accept(this))
    override def visit(e: CastToBoolean): Expression = e.copy(e.e.accept(this))
    override def visit(e: CastToString): Expression = e.copy(e.e.accept(this))
    override def visit(e: FunctionExpression): Expression = e.copy(arguments = e.arguments.map(_.accept(this)))
    override def visit(e: TryExpression): Expression = e.copy(e.toTry.accept(this), e.fallback.accept(this))
    override def visit(e: WithDefaultExpression): Expression = e.copy(e.expressions.map(_.accept(this)))
  }
}
