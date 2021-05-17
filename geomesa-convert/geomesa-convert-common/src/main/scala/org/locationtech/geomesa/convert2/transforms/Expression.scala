/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.EvaluationContext.{ContextDependent, FieldAccessor, NullFieldAccessor}
import org.locationtech.geomesa.convert2.Field

import scala.util.Try

sealed trait Expression extends ContextDependent[Expression] {
<<<<<<< HEAD

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[_ <: AnyRef]): AnyRef
=======

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[_ <: AnyRef]): AnyRef

  /**
    * Evaluate the expression against an input
    *
    * @param args arguments
    * @param ec evaluation context
    * @return
    */
  @deprecated("Use `withContext` and `apply`")
  def eval(args: Array[Any])(implicit ec: EvaluationContext): Any =
    withContext(ec).apply(args.asInstanceOf[Array[AnyRef]])
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)

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

  sealed trait Literal[T <: AnyRef] extends Expression {
    def value: T
    override def apply(args: Array[_ <: AnyRef]): AnyRef = value
    override def withContext(ec: EvaluationContext): Expression = this
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

  case class LiteralAny(value: AnyRef) extends Literal[AnyRef]

  case object LiteralNull extends Literal[AnyRef] { override def value: AnyRef = null }

  abstract class CastExpression(e: Expression, binding: String) extends Expression {
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      e.dependencies(stack, fieldMap)
    override def children(): Seq[Expression] = Seq(e)
    override def toString: String = s"$e::$binding"
  }

  case class CastToInt(e: Expression) extends CastExpression(e, "int") {
    override def apply(args: Array[_ <: AnyRef]): Integer = {
      e.apply(args) match {
        case n: Integer          => n
        case n: java.lang.Number => n.intValue()
        case n: String           => n.toInt
        case n: AnyRef           => n.toString.toInt
        case null                => throw new NullPointerException("Trying to cast 'null' to int")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToInt(ewc) }
    }
  }

  case class CastToLong(e: Expression) extends CastExpression(e, "long") {
    override def apply(args: Array[_ <: AnyRef]): java.lang.Long = {
      e.apply(args) match {
        case n: java.lang.Long   => n
        case n: java.lang.Number => n.longValue()
        case n: String           => n.toLong
        case n: AnyRef           => n.toString.toLong
        case null                => throw new NullPointerException("Trying to cast 'null' to long")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToLong(ewc) }
    }
  }

  case class CastToFloat(e: Expression) extends CastExpression(e, "float") {
    override def apply(args: Array[_ <: AnyRef]): java.lang.Float = {
      e.apply(args) match {
        case n: java.lang.Float  => n
        case n: java.lang.Number => n.floatValue()
        case n: String           => n.toFloat
        case n: AnyRef           => n.toString.toFloat
        case null                => throw new NullPointerException("Trying to cast 'null' to float")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToFloat(ewc) }
    }
  }

  case class CastToDouble(e: Expression) extends CastExpression(e, "double") {
    override def apply(args: Array[_ <: AnyRef]): java.lang.Double = {
      e.apply(args) match {
        case n: java.lang.Double => n
        case n: java.lang.Number => n.doubleValue()
        case n: String           => n.toDouble
        case n: AnyRef           => n.toString.toDouble
        case null                => throw new NullPointerException("Trying to cast 'null' to double")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToDouble(ewc) }
    }
  }

  case class CastToBoolean(e: Expression) extends CastExpression(e, "boolean") {
    override def apply(args: Array[_ <: AnyRef]): java.lang.Boolean = {
      e.apply(args) match {
        case b: java.lang.Boolean => b
        case b: String            => b.toBoolean
        case b: AnyRef            => b.toString.toBoolean
        case null                 => throw new NullPointerException("Trying to cast 'null' to boolean")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToBoolean(ewc) }
    }
  }

  case class CastToString(e: Expression) extends CastExpression(e, "string") {
    override def apply(args: Array[_ <: AnyRef]): String = {
      e.apply(args) match {
        case s: String => s
        case s: AnyRef => s.toString
        case null      => throw new NullPointerException("Trying to cast 'null' to String")
      }
    }
    override def withContext(ec: EvaluationContext): Expression = {
      val ewc = e.withContext(ec)
      if (e.eq(ewc)) { this } else { CastToString(ewc) }
    }
  }

  case class Column(i: Int) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = args(i)
    override def withContext(ec: EvaluationContext): Expression = this
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$$$i"
  }

  case class FieldLookup(n: String, accessor: FieldAccessor = NullFieldAccessor) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = accessor.apply()
    override def withContext(ec: EvaluationContext): Expression = FieldLookup(n, ec.accessor(n))
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
    override def apply(args: Array[_ <: AnyRef]): AnyRef = compiled
    override def withContext(ec: EvaluationContext): Expression = this
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] = Set.empty
    override def toString: String = s"$s::r"
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
  case class FunctionExpression(f: TransformerFunction, arguments: Array[Expression]) extends Expression {

    @volatile private var contextDependent: Int = -1

    private def this(f: TransformerFunction, arguments: Array[Expression], contextDependent: Int) = {
      this(f, arguments)
      this.contextDependent = contextDependent
    }

<<<<<<< HEAD
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
  case class FunctionExpression(
      f: TransformerFunction,
      arguments: Array[Expression],
      @volatile private var contextDependent: Int = -1
    ) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def withContext(ec: EvaluationContext): Expression = {
      // this code is thread-safe, in that it will ensure correctness, but does not guarantee
      // that the dependency check is only performed once
      if (contextDependent == 0) { this } else {
        lazy val fwc = f.withContext(ec)
        lazy val awc = arguments.map(_.withContext(ec))
        if (contextDependent == 1) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          if (!fwc.eq(f)) {
            contextDependent = 1
          } else {
            var i = 0
            while (i < arguments.length) {
              if (!awc(i).eq(arguments(i))) {
                contextDependent = 1
                i = Int.MaxValue
              } else {
                i += 1
              }
            }
            if (i == arguments.length) {
              contextDependent = 0
            }
          }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          if (contextDependent == 0) { this } else { new FunctionExpression(fwc, awc, 1) }
        }
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
          if (contextDependent == 0) { this } else { FunctionExpression(fwc, awc, 1) }
        }
      }
    }
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
<<<<<<< HEAD
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          if (contextDependent == 0) { this } else { new FunctionExpression(fwc, awc, 1) }
        }
      }
    }

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      arguments.flatMap(_.dependencies(stack, fieldMap)).toSet
    override def children(): Seq[Expression] = arguments
    override def toString: String = s"${f.names.head}${arguments.mkString("(", ",", ")")}"
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
  case class TryExpression(toTry: Expression, fallback: Expression) extends Expression {

    @volatile private var contextDependent: Int = -1

    private def this(toTry: Expression, fallback: Expression, contextDependent: Int) = {
      this(toTry, fallback)
      this.contextDependent = contextDependent
    }

<<<<<<< HEAD
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
  case class TryExpression(
      toTry: Expression,
      fallback: Expression,
      @volatile private var contextDependent: Int = -1
    ) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def withContext(ec: EvaluationContext): Expression = {
      // this code is thread-safe, in that it will ensure correctness, but does not guarantee
      // that the dependency check is only performed once
      if (contextDependent == 0) { this } else {
        lazy val twc = toTry.withContext(ec)
        lazy val fwc = fallback.withContext(ec)
        if (contextDependent == 1) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new TryExpression(twc, fwc, 1)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
          TryExpression(twc, fwc, 1)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
=======
          TryExpression(twc, fwc, 1)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      toTry.dependencies(stack, fieldMap) ++ fallback.dependencies(stack, fieldMap)
    override def children(): Seq[Expression] = Seq(toTry, fallback)
    override def toString: String = s"try($toTry,$fallback)"
  }
}
