/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.Field

import scala.util.Try

trait Expression {

  /**
    * Evaluate the expression against an input
    *
    * @param args arguments
    * @param ctx evaluation context
    * @return
    */
  def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any

  /**
    * Gets the field dependencies that this expr relies on
    *
    * @param stack current field stack, used to detect circular dependencies
    * @param fieldMap fields lookup
    * @return dependencies
    */
  def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field]

  /**
    * Any nested expressions
    *
    * @return
    */
  def children(): Seq[Expression] = Seq.empty
}

object Expression {

  def apply(e: String): Expression = ExpressionParser.parse(e)

  /**
    * Returns the list of unique expressions in the input, including any descendants
    *
    * @param expressions expressions
    * @return
    */
  def flatten(expressions: Seq[Expression]): Seq[Expression] = {
    val toCheck = scala.collection.mutable.Queue(expressions: _*)
    val result = scala.collection.mutable.Set.empty[Expression]
    while (toCheck.nonEmpty) {
      val next = toCheck.dequeue()
      if (result.add(next)) {
        toCheck ++= next.children()
      }
    }
    result.toSeq
  }

  sealed trait Literal[T <: Any] extends Expression {
    def value: T
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = value
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = String.valueOf(value)
  }

  case class LiteralString(value: String) extends Literal[String] {
    override def toString: String = s"'${String.valueOf(value)}'"
  }

  case class LiteralInt(value: Integer) extends Literal[Integer]

  case class LiteralLong(value: java.lang.Long) extends Literal[java.lang.Long]

  case class LiteralFloat(value: java.lang.Float) extends Literal[java.lang.Float]

  case class LiteralDouble(value: java.lang.Double) extends Literal[java.lang.Double]

  case class LiteralBoolean(value: java.lang.Boolean) extends Literal[java.lang.Boolean]

  case class LiteralAny(value: Any) extends Literal[Any]

  case object LiteralNull extends Literal[AnyRef] { override def value: AnyRef = null }

  abstract class CastExpression(e: Expression, binding: String) extends Expression {
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      e.dependencies(stack, fieldMap)
    override def children(): Seq[Expression] = Seq(e)
    override def toString: String = s"$e::$binding"
  }

  case class CastToInt(e: Expression) extends CastExpression(e, "int") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Int =
      e.eval(args) match {
        case n: Int    => n
        case n: Double => n.toInt
        case n: Float  => n.toInt
        case n: Long   => n.toInt
        case n: Any    => n.toString.toInt
      }
  }

  case class CastToLong(e: Expression) extends CastExpression(e, "long") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Long =
      e.eval(args) match {
        case n: Int    => n.toLong
        case n: Double => n.toLong
        case n: Float  => n.toLong
        case n: Long   => n
        case n: Any    => n.toString.toLong
      }
  }

  case class CastToFloat(e: Expression) extends CastExpression(e, "float") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Float =
      e.eval(args) match {
        case n: Int    => n.toFloat
        case n: Double => n.toFloat
        case n: Float  => n
        case n: Long   => n.toFloat
        case n: Any    => n.toString.toFloat
      }
  }

  case class CastToDouble(e: Expression) extends CastExpression(e, "double") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Double =
      e.eval(args) match {
        case n: Int    => n.toDouble
        case n: Double => n
        case n: Float  => n.toDouble
        case n: Long   => n.toDouble
        case n: Any    => n.toString.toDouble
      }
  }

  case class CastToBoolean(e: Expression) extends CastExpression(e, "boolean") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Boolean = e.eval(args).toString.toBoolean
  }

  case class CastToString(e: Expression) extends CastExpression(e, "string") {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): String = e.eval(args).toString
  }

  case class Column(i: Int) extends Expression {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = args(i)
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$$$i"
  }

  case class FieldLookup(n: String) extends Expression {
    private var doEval: EvaluationContext => Any = ctx => {
      val idx = ctx.indexOf(n)
      // rewrite the eval to lookup by index
      doEval = if (idx < 0) { _  => null } else { ec => ec.get(idx) }
      doEval(ctx)
    }

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = doEval(ctx)

    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = {
      fieldMap.get(n) match {
        case None => Set.empty
        case Some(field) =>
          if (stack.contains(field)) {
            throw new IllegalArgumentException(s"Cyclical dependency detected in field $field")
          } else {
            field.transforms.toSeq.flatMap(_.dependencies(stack + field, fieldMap)).toSet + field
          }
      }
    }

    override def toString: String = s"$$$n"
  }

  case class RegexExpression(s: String) extends Expression {
    private val compiled = s.r
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = compiled
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$s::r"
  }

  case class FunctionExpression(f: TransformerFunction, arguments: Array[Expression]) extends Expression {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      f.eval(arguments.map(_.eval(args)))
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      arguments.flatMap(_.dependencies(stack, fieldMap)).toSet
    override def children(): Seq[Expression] = arguments
    override def toString: String = s"${f.names.head}${arguments.mkString("(", ",", ")")}"
  }

  case class TryExpression(toTry: Expression, fallback: Expression) extends Expression {
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any =
      Try(toTry.eval(args)).getOrElse(fallback.eval(args))
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      toTry.dependencies(stack, fieldMap) ++ fallback.dependencies(stack, fieldMap)
    override def children(): Seq[Expression] = Seq(toTry, fallback)
    override def toString: String = s"try($toTry,$fallback)"
  }
}
