/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import com.esotericsoftware.kryo.io.Input
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.PathFilter._
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{JsonPath, PathFilter}
import org.locationtech.geomesa.features.kryo.json.KryoJsonPath.ValuePointer

import java.util.regex.Pattern

trait KryoJsonPathFilter extends (ValuePointer => Boolean)

object KryoJsonPathFilter {

  /**
   * Create a filter
   *
   * @param in input
   * @param root root node of the document being evaluated (for absolute path references)
   * @param op the filter op
   * @return
   */
  def apply(in: Input, root: ValuePointer, op: PathFilter.FilterOp): KryoJsonPathFilter =
    new FilterBuilder(in, root)(op)

  /**
   * An expression in a filter - either a path, or a literal
   */
  private type Expression = ValuePointer => Any

  /**
   * Builder
   *
   * @param in input
   * @param root root document path (for absolute path expressions)
   */
  private class FilterBuilder(in: Input, root: ValuePointer) {

    def apply(op: PathFilter.FilterOp): KryoJsonPathFilter = op match {
      case EqualsOp(left, right)              => BinaryFilter.Equals(expression(left), expression(right))
      case NotEqualsOp(left, right)           => BinaryFilter.NotEquals(expression(left), expression(right))
      case LessThanOp(left, right)            => ComparisonFilter.LessThan(expression(left), expression(right))
      case LessThanOrEqualsOp(left, right)    => ComparisonFilter.LessThanOrEquals(expression(left), expression(right))
      case GreaterThanOp(left, right)         => ComparisonFilter.GreaterThan(expression(left), expression(right))
      case GreaterThanOrEqualsOp(left, right) => ComparisonFilter.GreaterThanOrEquals(expression(left), expression(right))
      case RegexOp(path, regex)               => RegexFilter(expression(path), regex)
      case InOp(left, right)                  => ScalarArrayFilter.In(expression(left), expression(right))
      case NotInOp(left, right)               => ScalarArrayFilter.NotIn(expression(left), expression(right))
      case IsSubsetOp(left, right)            => ArrayFilter.Subset(expression(left), expression(right))
      case AnyOfOp(left, right)               => ArrayFilter.AnyOf(expression(left), expression(right))
      case NoneOfOp(left, right)              => ArrayFilter.NoneOf(expression(left), expression(right))
      case EqualSizeOp(left, right)           => SizeFilter(expression(left), expression(right))
      case IsEmptyOp(value, empty)            => IsEmptyFilter(expression(value), expression(empty))
      case ExistsOp(value)                    => ExistsFilter(expression(value))
      case AndFilterOp(filters)               => AndFilter(filters.map(this.apply))
      case OrFilterOp(filters)                => OrFilter(filters.map(this.apply))
      case NotFilterOp(filter)                => NotFilter(this.apply(filter))
    }

    private def expression(exp: FilterExpression): Expression = exp match {
      case p: PathExpression =>
        if (p.path.elements.isEmpty) {
          Constant(null)
        } else if (p.absolute) {
          Constant(new KryoJsonPath(in, root).deserialize(p.path))
        } else {
          Path(in, p.path)
        }
      case lit: LiteralExpression =>
        Constant(lit.value)
    }
  }

  /**
   * Constant value - does not change if the input changes
   *
   * @param value value
   */
  private case class Constant(value: Any) extends Expression {
    override def apply(pointer: ValuePointer): Any = value
  }

  /**
   * Path expression
   *
   * @param in input
   * @param path path to evaluate
   */
  private case class Path(in: Input, path: JsonPath) extends Expression {
    override def apply(pointer: ValuePointer): Any = new KryoJsonPath(in, pointer).deserialize(path)
  }

  /**
   * Filter op between two arbitrary expressions
   *
   * @param left left expression
   * @param right right expression
   * @param op comparison op
   */
  private case class BinaryFilter(left: Expression, right: Expression, op: BinaryFilter.BinaryPredicate)
      extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = op(left(pointer), right(pointer))
  }

  private object BinaryFilter {
    trait BinaryPredicate extends ((Any, Any) => Boolean) {
      // constructor
      def apply(left: Expression, right: Expression): BinaryFilter = BinaryFilter(left, right, this)
    }

    object Equals extends BinaryPredicate {
      override def apply(left: Any, right: Any): Boolean = left == right
    }
    object NotEquals extends BinaryPredicate {
      override def apply(left: Any, right: Any): Boolean = left != right
    }
  }

  /**
   * Filter op between two 'comparable' expressions - expressions must both evaluate to either strings or numbers
   *
   * @param left left expression
   * @param right right expression
   * @param op comparison op
   */
  private case class ComparisonFilter(left: Expression, right: Expression, op: ComparisonFilter.ComparisonPredicate)
    extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = {
      left(pointer) match {
        case leftValue: Number =>
          right(pointer) match {
            case rightValue: Number => op(leftValue.doubleValue(), rightValue.doubleValue())
            case _ => false
          }
        case leftValue: String =>
          right(pointer) match {
            case rightValue: String => op(leftValue, rightValue)
            case _ => false
          }
        case _ => false
      }
    }
  }

  private object ComparisonFilter {

    trait ComparisonPredicate {
      // constructor
      def apply(left: Expression, right: Expression): ComparisonFilter = ComparisonFilter(left, right, this)
      def apply(left: Double, right: Double): Boolean
      def apply(left: String, right: String): Boolean
    }

    object LessThan extends ComparisonPredicate {
      override def apply(left: Double, right: Double): Boolean = left < right
      override def apply(left: String, right: String): Boolean = left < right
    }
    object LessThanOrEquals extends ComparisonPredicate {
      override def apply(left: Double, right: Double): Boolean = left <= right
      override def apply(left: String, right: String): Boolean = left <= right
    }
    object GreaterThan extends ComparisonPredicate {
      override def apply(left: Double, right: Double): Boolean = left > right
      override def apply(left: String, right: String): Boolean = left > right
    }
    object GreaterThanOrEquals extends ComparisonPredicate {
      override def apply(left: Double, right: Double): Boolean = left >= right
      override def apply(left: String, right: String): Boolean = left >= right
    }
  }

  /**
   * Regex filter op
   *
   * `=~` - left matches regular expression `[?(@.name =~ /foo.*?/i)]`
   *
   * @param left path expression, must evaluate to a string
   * @param right regex to match
   */
  private case class RegexFilter(left: Expression, right: Pattern) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = {
      left(pointer) match {
        case s: String => right.matcher(s).matches()
        case _ => false
      }
    }
  }

  private trait ArrayOps {
    import scala.collection.JavaConverters._

    def toSeq(expression: Any): Option[Seq[Any]] = expression match {
      case s: Seq[_] => Some(s)
      case s: java.util.List[_] => Some(s.asScala.toSeq)
      case _ => None
    }

    def toString(expression: Any): Option[Seq[Any]] = expression match {
      case s: String => Some(s)
      case _ => None
    }
  }

  /**
   * Filter op between a scalar (number, string) and an array
   *
   * @param left scalar expression
   * @param right array expression
   * @param op comparison op
   */
  private case class ScalarArrayFilter(left: Expression, right: Expression, op: ScalarArrayFilter.ScalarArrayPredicate)
    extends KryoJsonPathFilter with ArrayOps {
    override def apply(pointer: ValuePointer): Boolean = toSeq(right(pointer)).exists(op(left(pointer), _))
  }

  private object ScalarArrayFilter {

    trait ScalarArrayPredicate extends ((Any, Seq[Any]) => Boolean) {
      // constructor
      def apply(left: Expression, right: Expression): ScalarArrayFilter = ScalarArrayFilter(left, right, this)
    }

    // in - left exists in right [?(@.size in ['S', 'M'])]
    object In extends ScalarArrayPredicate {
      override def apply(left: Any, right: Seq[Any]): Boolean = right.contains(left)
    }

    // nin - left does not exists in right
    object NotIn extends ScalarArrayPredicate {
      override def apply(left: Any, right: Seq[Any]): Boolean = !right.contains(left)
    }
  }

  /**
   * Filter op between two arrays
   *
   * @param left left array
   * @param right right array
   * @param op comparison op
   */
  private case class ArrayFilter(left: Expression, right: Expression, op: ArrayFilter.ArrayPredicate)
      extends KryoJsonPathFilter with ArrayOps {
    override def apply(pointer: ValuePointer): Boolean = {
      val seqs = for { s1 <- toSeq(left(pointer)); s2 <- toSeq(right(pointer)) } yield { op(s1, s2) }
      seqs.getOrElse(false)
    }
  }

  private object ArrayFilter {

    trait ArrayPredicate extends ((Seq[Any], Seq[Any]) => Boolean) {
      // constructor
      def apply(left: Expression, right: Expression): ArrayFilter = ArrayFilter(left, right, this)
    }

    // subsetof - left is a subset of right [?(@.sizes subsetof ['S', 'M', 'L'])]
    object Subset extends ArrayPredicate {
      override def apply(left: Seq[Any], right: Seq[Any]): Boolean = left.forall(right.contains)
    }

    // anyof - left has an intersection with right [?(@.sizes anyof ['M', 'L'])]
    object AnyOf extends ArrayPredicate {
      override def apply(left: Seq[Any], right: Seq[Any]): Boolean = left.exists(right.contains)
    }

    //noneof - left has no intersection with right [?(@.sizes noneof ['M', 'L'])]
    object NoneOf extends ArrayPredicate {
      override def apply(left: Seq[Any], right: Seq[Any]): Boolean = !left.exists(right.contains)
    }
  }

  /**
   * Size filter op
   *
   * `size` - size of left (array or string) should match right
   *
   * @param left left expression, array or string
   * @param right right expression, array or string
   */
  private case class SizeFilter(left: Expression, right: Expression) extends KryoJsonPathFilter with ArrayOps {
    override def apply(pointer: ValuePointer): Boolean = {
      val leftValue = left(pointer)
      val rightValue = right(pointer)
      val arraySize = for { s1 <- toSeq(leftValue); s2 <- toSeq(rightValue) } yield { s1.size == s2.size }
      lazy val stringSize = for { s1 <- toString(leftValue); s2 <- toString(rightValue) } yield { s1.size == s2.size }
      arraySize.orElse(stringSize).getOrElse(false)
    }
  }

  /**
   * Empty check filter op
   *
   * `empty` - left (array or string) should be empty
   *
   * @param value array or string
   * @param empty empty or not empty
   */
  private case class IsEmptyFilter(value: Expression, empty: Expression) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = {
      empty(pointer) match {
        case b: Boolean =>
          value(pointer) match {
            case s: Seq[_] => s.isEmpty == b
            case s: java.util.List[_] => s.isEmpty == b
            case s: String => s.isEmpty == b
            case null => b
            case _ => false
          }
        case _ => false
      }
    }
  }

  /**
   * Filter op for whether expression exists (i.e. is not null)
   *
   * @param value any expression
   */
  private case class ExistsFilter(value: Expression) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = value(pointer) != null
  }

  private case class AndFilter(filters: Seq[KryoJsonPathFilter]) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = filters.forall(_.apply(pointer))
  }
  private case class OrFilter(filters: Seq[KryoJsonPathFilter]) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = filters.exists(_.apply(pointer))
  }
  private case class NotFilter(filter: KryoJsonPathFilter) extends KryoJsonPathFilter {
    override def apply(pointer: ValuePointer): Boolean = !filter.apply(pointer)
  }
}
