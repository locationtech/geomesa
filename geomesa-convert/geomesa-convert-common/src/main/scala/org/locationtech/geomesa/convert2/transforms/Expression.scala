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
<<<<<<< HEAD
<<<<<<< HEAD

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[_ <: AnyRef]): AnyRef
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
=======
=======
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[_ <: AnyRef]): AnyRef
=======
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)

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
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)

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
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)

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
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)

  /**
   * Evaluate the expression against an input row
   *
   * @param args arguments
   * @return
   */
  def apply(args: Array[_ <: AnyRef]): AnyRef

  /**
<<<<<<< HEAD
    * Evaluate the expression against an input
    *
    * @param args arguments
    * @param ec evaluation context
    * @return
    */
  @deprecated("Use `withContext` and `apply`")
  def eval(args: Array[Any])(implicit ec: EvaluationContext): Any =
    withContext(ec).apply(args.asInstanceOf[Array[AnyRef]])
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)

  /**
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
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
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
  case class FunctionExpression(
      f: TransformerFunction,
      arguments: Array[Expression],
      @volatile private var contextDependent: Int = -1
    ) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = f.apply(arguments.map(_.apply(args)))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
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
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
          new FunctionExpression(fwc, awc, 1)
=======
          FunctionExpression(fwc, awc, 1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
          new FunctionExpression(fwc, awc, 1)
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          if (contextDependent == 0) { this } else { new FunctionExpression(fwc, awc, 1) }
        }
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
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
          if (contextDependent == 0) { this } else { FunctionExpression(fwc, awc, 1) }
        }
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
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
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
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
          if (contextDependent == 0) { this } else { new FunctionExpression(fwc, awc, 1) }
        }
      }
    }

>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
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
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
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
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
  case class TryExpression(
      toTry: Expression,
      fallback: Expression,
      @volatile private var contextDependent: Int = -1
    ) extends Expression {
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
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
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
    override def apply(args: Array[_ <: AnyRef]): AnyRef = Try(toTry.apply(args)).getOrElse(fallback.apply(args))

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3605e1a51 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8e2dfa5c2 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d39a02f21 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e8ce62d0d6 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8871ac11c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8963a21317 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3ff426afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 776ee41b60 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 24df6d87c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e0cdfec1d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0c38d9736 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d05e4adb4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======

>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ed35b9ff9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b6c4628dba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249aba (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57edc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 7cd2c4188b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
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
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afcc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ff5c21d0c5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3605e1a519 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d39a02f21d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8871ac11cf (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3ff426afc7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24df6d87cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0c38d97369 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 0735939d3a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0df4ab56df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
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
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cf42b98f8f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
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
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1562748d0b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff1 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5e17a3f871 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 364812dd7e (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59c99fa93a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
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
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 55f794c0f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 009ad2a267 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f39309c33 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5eb2c4c6ae (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 78e21a715 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b1512df467 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 69d749947 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6d9ef35086 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> dab28922a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 59d9fe5f6a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 7b5119937 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2c62d0c202 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 9064032df (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1d3562f96d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 4108c4b7d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 340ce6ce0a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 09ccf06d4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 548538f30d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 98e25fe678 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 819897efd9 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
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
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ff5c21d0c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 562f547289 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 78fa7e3fee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
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
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 357511344f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 5b1a07faee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 1c5c649655 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> cb5b96559a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 069863cf49 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 5d1bb9b74c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 0735939d3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> e85a6da03f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d53e5acc27 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
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
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 52c9856ea (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fdeed2b5f7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b618fc3e64 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
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
<<<<<<< HEAD
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 0df4ab56d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdc7c0fe7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 90704d8cb5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
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
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cf42b98f8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b9ad16da06 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2e9f0a646 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
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
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
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
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1f8411bc8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2b0ee2792f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d3687dfc62 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 1562748d0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2e8312e059 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 87b5d3f05a (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6b6969a79 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d50db57b4d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 4c0ecd2967 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ab4d758ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 822418da7f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0305822bf4 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0088e6eff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> d40b2ba4c8 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 665c8e5c85 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5e17a3f87 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 27c16f3904 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> cdf2b37114 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
>>>>>>> 364812dd7 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f038e6d420 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 66bb677567 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
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
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> c8e80535d (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3e85ab9274 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> fa73d5f3fd (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
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
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 59c99fa93 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 6f38f52008 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> ca6c774652 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
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
=======
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> dca5d74b69 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
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
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 3a2e956065 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 8d9c3c2d16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 65ba54511b (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 1a54249ab (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> f8fd014f1f (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 8f88e57ed (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 0970537f16 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 059393960 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 7cd2c4188 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> b2cada94f0 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> b6c4628db (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5e8ce12ec (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 5f17de37ee (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
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
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> aada4d63c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
<<<<<<< HEAD
>>>>>>> 2024fa80f3 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
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
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======

>>>>>>> 1108247cc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 6ed35b9ff (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 9a1ca2afc (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 5fae301729 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
          TryExpression(twc, fwc, 1)
=======
          new TryExpression(twc, fwc, 1)
>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
        } else {
          contextDependent = if (twc.eq(toTry) && fwc.eq(fallback)) { 0 } else { 1 }
          if (contextDependent == 0) { this } else { new TryExpression(twc, fwc, 1) }
        }
      }
    }
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======

>>>>>>> 1108247cc5 (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
>>>>>>> 16a2ad178c (GEOMESA-3076 Do inexact checking for cardinality values in minmax stat)
    override def dependencies(stack: Set[Field], fieldMap: Map[String, Field]): Set[Field] =
      toTry.dependencies(stack, fieldMap) ++ fallback.dependencies(stack, fieldMap)
    override def children(): Seq[Expression] = Seq(toTry, fallback)
    override def toString: String = s"try($toTry,$fallback)"
  }
}
