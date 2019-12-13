/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2
import org.locationtech.geomesa.convert2.transforms.Expression.Column
import org.locationtech.geomesa.convert2.transforms._

@deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.Expression and org.locationtech.geomesa.convert2.transforms.Predicate")
object Transformers extends LazyLogging {

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.Expression")
  trait Expr extends org.locationtech.geomesa.convert2.transforms.Expression {

    /**
      * Gets the field dependencies that this expr relies on
      *
      * @param stack current field stack, used to detect circular dependencies
      * @param fieldNameMap fields lookup
      * @return dependencies
      */
    def dependenciesOf(stack: Set[Field], fieldNameMap: Map[String, Field]): Set[Field] =
      dependencies(stack.asInstanceOf[Set[convert2.Field]], fieldNameMap).map(FieldWrapper.apply)
  }

  /**
    * Wrapper to convert new API expressions to old API
    *
    * @param expr expression
    */
  case class ExpressionWrapper(expr: Expression) extends Expr {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = expr.eval(args)(ctx)
    override def dependencies(stack: Set[convert2.Field], fieldMap: Map[String, convert2.Field]): Set[convert2.Field] =
      expr.dependencies(stack, fieldMap)
  }

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.Predicate")
  trait Predicate extends org.locationtech.geomesa.convert2.transforms.Predicate

  case class PredicateWrapper(pred: org.locationtech.geomesa.convert2.transforms.Predicate) extends Predicate {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = pred.eval(args)(ctx)
  }

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.Expression.Column")
  case class Col(i: Int) extends Expr {
    private val delegate = Column(i)
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = delegate.eval(args)(ctx)
    override def dependencies(stack: Set[convert2.Field], fieldMap: Map[String, convert2.Field]): Set[convert2.Field] =
      delegate.dependencies(stack, fieldMap)
  }

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.Expression.FieldLookup")
  case class FieldLookup(n: String) extends Expr {
    private val delegate = org.locationtech.geomesa.convert2.transforms.Expression.FieldLookup(n)
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = delegate.eval(args)(ctx)
    override def dependencies(stack: Set[convert2.Field], fieldMap: Map[String, convert2.Field]): Set[convert2.Field] =
      delegate.dependencies(stack, fieldMap)
  }

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.ExpressionParser")
  def parseTransform(s: String): Expr = ExpressionWrapper(Expression(s))

  @deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.PredicateParser")
  def parsePred(s: String): Predicate = PredicateWrapper(Predicate(s))
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.TransformerFunction")
object TransformerFn {
  def apply(n: String*)(f: (Array[Any]) => Any): TransformerFn = new TransformerFn {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = f(args)
    override def names: Seq[String] = n
  }
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.TransformerFunction")
trait TransformerFn extends org.locationtech.geomesa.convert2.transforms.TransformerFunction

@deprecated("Replaced with org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory")
trait TransformerFunctionFactory extends org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory {
  def functions: Seq[TransformerFn]
}
