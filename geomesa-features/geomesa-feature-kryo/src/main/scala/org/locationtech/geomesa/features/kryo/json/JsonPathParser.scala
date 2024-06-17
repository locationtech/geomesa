/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.apache.commons.text.StringEscapeUtils
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.Context
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala._

import java.util.Objects
import java.util.regex.Pattern

/**
  * Parses a json path string into a sequence of selectors. See https://github.com/jayway/JsonPath for examples.
  */
object JsonPathParser {

  private val Parser = new JsonPathParser()

  @throws(classOf[ParsingException])
  def parse(path: String, report: Boolean = true): JsonPath = {
    if (path == null) {
      throw new IllegalArgumentException("Path must not be null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.Path) } else { BasicParseRunner(Parser.Path) }
    val fixedPath = if (path.startsWith("$")) { path } else s"$$.$path"
    val parsing = runner.run(fixedPath)
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid json path: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  /**
   * Prints a parsed path back into a path string
   *
   * @param path path
   * @param prefix prefix
   * @return
   */
  def print(path: JsonPath, prefix: Char = '$'): String = {
    require(path.elements.nonEmpty, "Path must not be empty")

    val builder = new StringBuilder()

    def append(e: Any): Unit = {
      val s = e.toString
      // handle deep scans followed by dot selectors
      if (s.startsWith(".") && builder.endsWith(Seq('.', '.'))) {
        builder.append(s.substring(1))
      } else {
        builder.append(s)
      }
    }

    builder.append(prefix)
    path.elements.foreach(append)
    path.function.foreach(append)
    builder.toString()
  }

  /**
   * A parsed json path
   *
   * @param elements selector elements in the path
   * @param function an optional trailing function (per spec, may only be 1 trailing function in a path)
   */
  case class JsonPath(elements: Seq[PathElement], function: Option[PathFunction] = None) {
    def isEmpty: Boolean = elements.isEmpty
    def nonEmpty: Boolean = elements.nonEmpty
    def head: PathElement = elements.head
    def tail: JsonPath = JsonPath(elements.tail, function)
  }

  sealed trait PathElement

  // attribute: .foo or ['foo']
  case class PathAttribute(name: String, bracketed: Boolean = false) extends PathElement {
    override def toString: String = if (bracketed) { s"['$name']" } else { s".$name" }
  }

  // index range [1:5] (inclusive on start, exclusive on end, either side can be empty to indicate from start/to end)
  case class PathIndexRange(from: Option[Int], to: Option[Int]) extends PathElement {
    override def toString: String = s"[${from.getOrElse("")}:${to.getOrElse("")}]"
  }

  // enumerated indices: [1,2,5]
  case class PathIndices(indices: Seq[Int]) extends PathElement {
    override def toString: String = indices.mkString("[", ",", "]")
  }

  // any attribute: .*
  case object PathAttributeWildCard extends PathElement {
    override val toString: String = ".*"
  }

  // any index: [*]
  case object PathIndexWildCard extends PathElement {
    override val toString: String = "[*]"
  }

  // deep scan: ..
  case object PathDeepScan extends PathElement {
    override val toString: String = ".."
  }

  // path function: .min(), .max(), .avg(), .length(), .sum(), .first(), .last()
  // not implemented: stddev, concat, append, keys
  sealed trait PathFunction extends (Any => Any)

  object PathFunction {

    import scala.collection.JavaConverters._

    /**
     * Function over a seq of numbers
     *
     * @param name name of the function
     * @param op operation
     */
    abstract class NumericFunction(name: String, op: Seq[Double] => Double) extends PathFunction {
      private def toNum(v: Any): Double = v match {
        case n: Number => n.doubleValue
        case null => 0.0
        case n => n.toString.toDouble
      }
      override def apply(args: Any): Any = {
        args match {
          case s: java.util.List[_] if !s.isEmpty => op(s.asScala.toSeq.map(toNum))
          case s: Seq[_] if s.nonEmpty => op(s.map(toNum))
          case _ => null
        }
      }
      override def toString: String = s".$name()"
    }

    case object MinFunction extends NumericFunction("min", _.min)
    case object MaxFunction extends NumericFunction("max", _.max)
    case object SumFunction extends NumericFunction("sum", _.sum)
    case object AvgFunction extends NumericFunction("avg", a => a.sum / a.length)

    case object LengthFunction extends PathFunction {
      override def apply(obj: Any): Any = obj match {
        case s: java.util.List[_] => s.size
        case s: Seq[_] => s.size
        case s: String => s.length
        case _ => null
      }
      override def toString: String = ".length()"
    }

    case object FirstFunction extends PathFunction {
      override def apply(obj: Any): Any = obj match {
        case s: java.util.List[_] if !s.isEmpty => s.get(0)
        case s: Seq[_] if s.nonEmpty => s.head
        case s: String if s.nonEmpty => s.substring(0, 1)
        case _ => null
      }
      override def toString: String = ".first()"
    }

    case object LastFunction extends PathFunction {
      override def apply(obj: Any): Any = obj match {
        case s: java.util.List[_] if !s.isEmpty => s.get(s.size() - 1)
        case s: Seq[_] if s.nonEmpty => s.last
        case s: String if s.nonEmpty => s.substring(s.length - 1, s.length)
        case _ => null
      }
      override def toString: String = ".last()"
    }

    // note: negative indices count backwards from the end
    case class IndexFunction(i: Int) extends PathFunction {
      override def apply(obj: Any): Any = obj match {
        case s: java.util.List[_] if i >= 0 && i < s.size => s.get(i)
        case s: java.util.List[_] if i < 0 && s.size + i >= 0 => s.get(s.size + i)
        case s: Seq[_] if i >= 0 && i < s.size => s(i)
        case s: Seq[_] if i < 0 && s.size + i >= 0 => s(s.size + i)
        case s: String if i >= 0 && i < s.length => s.substring(i, i + 1)
        case s: String if i < 0 && s.length + i >= 0 => s.substring(0, s.length + i + 1)
        case _ => null
      }
      override def toString: String = s".index($i)"
    }
  }

  // filter ops: [?(<exp>)]
  case class PathFilter(op: PathFilter.FilterOp) extends PathElement {
    override def toString: String = {
      val filter = op.toString
      if (filter.startsWith("(") && filter.endsWith(")")) { s"[?$filter]" } else { s"[?($filter)]" }
    }
  }

  object PathFilter {

    /**
     * A filter operation (equals, less than, etc)
     */
    sealed trait FilterOp

    // == left is equal to right (note that 1 is not equal to '1')
    case class EqualsOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left == $right"
    }

    // != left is not equal to right
    case class NotEqualsOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left != $right"
    }

    // < left is less than right
    case class LessThanOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left < $right"
    }

    // <= left is less or equal to right
    case class LessThanOrEqualsOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left <= $right"
    }

    // > left is greater than right
    case class GreaterThanOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left > $right"
    }

    // >= left is greater than or equal to right
    case class GreaterThanOrEqualsOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left >= $right"
    }

    // =~ left matches regular expression [?(@.name =~ /foo.*?/i)]
    case class RegexOp(left: PathExpression, regex: Pattern) extends FilterOp {

      override def toString: String = {
        val flag = if ((regex.flags() & Pattern.CASE_INSENSITIVE) != 0) { "i" } else { "" }
        s"$left =~ /${regex.pattern()}/$flag"
      }

      // note: we implement equals and hash code since Pattern does not (mainly for unit tests)
      override def hashCode(): Int = Objects.hash(left, regex.pattern(), Int.box(regex.flags()))

      override def equals(obj: Any): Boolean = obj match {
        case r: RegexOp => left == r.left && regex.pattern() == r.regex.pattern() && regex.flags() == r.regex.flags()
        case _ => false
      }

      override def canEqual(that: Any): Boolean = that.isInstanceOf[RegexOp]
    }

    // in left exists in right [?(@.size in ['S', 'M'])]
    case class InOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left in $right"
    }

    // nin left does not exists in right
    case class NotInOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left nin $right"
    }

    // subsetof left is a subset of right [?(@.sizes subsetof ['S', 'M', 'L'])]
    case class IsSubsetOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left subsetof $right"
    }

    // anyof left has an intersection with right [?(@.sizes anyof ['M', 'L'])]
    case class AnyOfOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left anyof $right"
    }

    // noneof left has no intersection with right [?(@.sizes noneof ['M', 'L'])]
    case class NoneOfOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left noneof $right"
    }

    // size size of left (array or string) should match right
    case class EqualSizeOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left size $right"
    }

    // empty left (array or string) should be empty (specified by right)
    case class IsEmptyOp(left: FilterExpression, right: FilterExpression) extends FilterOp {
      override def toString: String = s"$left empty $right"
    }

    // value exists
    case class ExistsOp(value: FilterExpression) extends FilterOp {
      override def toString: String = value.toString
    }

    case class AndFilterOp(filters: Seq[FilterOp]) extends FilterOp {
      override def toString: String = filters.mkString("(", " && ", ")")
    }

    case class OrFilterOp(filters: Seq[FilterOp]) extends FilterOp {
      override def toString: String = filters.mkString("(", " || ", ")")
    }

    case class NotFilterOp(filter: FilterOp) extends FilterOp {
      override def toString: String = s"!$filter"
    }

    /**
     * An argument in a filter
     */
    sealed trait FilterExpression

    /**
     * A json-path argument in a filter
     *
     * @param path json path
     * @param absolute relative to current node, or absolute (@ vs $)
     */
    case class PathExpression(path: JsonPath, absolute: Boolean = false) extends FilterExpression {
      override def toString: String = JsonPathParser.print(path, if (absolute) { '$' } else { '@' })
    }

    /**
     * A literal argument in a filter
     */
    sealed trait LiteralExpression extends FilterExpression {
      def value: Any
    }

    case class NumericLiteral(value: BigDecimal) extends LiteralExpression {
      override def toString: String = value.toString
    }
    case class StringLiteral(value: String) extends LiteralExpression {
      override def toString: String = s"'${value.replaceAll("'", "\\'")}'"
    }
    case class BooleanLiteral(value: Boolean) extends LiteralExpression {
      override def toString: String = value.toString
    }
    case class ArrayLiteral[T <: LiteralExpression](wrapped: Seq[T]) extends LiteralExpression {
      override val value: Seq[Any] = wrapped.map(_.value)
      override def toString: String = wrapped.mkString("[", ", ", "]")
    }
  }
}

private class JsonPathParser extends BasicParser {

  import PathFilter._

  // main parsing rule
  def Path: Rule1[JsonPath] =
    rule { "$" ~ zeroOrMore(Element) ~ optional(Function) ~~> ((e, f) => JsonPath(e, f)) ~ EOI }

  def Element: Rule1[PathElement] = rule {
    Attribute | ArrayIndices | ArrayIndexRange | BracketedAttribute | AttributeWildCard | IndexWildCard | DeepScan | FilterOp
  }

  def IndexWildCard: Rule1[PathElement] = rule { "[*]" ~ push(PathIndexWildCard) }

  def AttributeWildCard: Rule1[PathElement] = rule { ".*" ~ push(PathAttributeWildCard) }

  // we have to push the deep scan directly onto the stack as there is no forward matching and
  // it's ridiculous trying to combine Rule1's and Rule2's
  def DeepScan: Rule1[PathElement] = rule { "." ~ toRunAction(pushDeepScan) ~ (Attribute | BracketedAttribute | AttributeWildCard) }

  // note: this assumes that we are inside a zeroOrMore, which is currently the case
  // the zeroOrMore will have pushed a single list onto the value stack - we append our value to that
  private def pushDeepScan(context: Context[Any]): Unit =
    context.getValueStack.push(PathDeepScan :: context.getValueStack.pop.asInstanceOf[List[_]])

  def ArrayIndices: Rule1[PathIndices] =
    rule { "[" ~ oneOrMore(int, ",") ~ "]" ~~> PathIndices.apply }

  def ArrayIndexRange: Rule1[PathIndexRange] =
    rule { "[" ~ optional(int) ~ ":" ~ optional(int) ~ "]" ~~> ((from, to) => PathIndexRange(from, to)) }

  def Attribute: Rule1[PathAttribute] = rule { "." ~ oneOrMore(char) ~> { s => PathAttribute(s) } ~ !"(" }

  def BracketedAttribute: Rule1[PathAttribute] =
    rule { "[" ~ (unquotedString | singleQuotedString) ~~> { s => PathAttribute(s, bracketed = true) } ~ "]" }

  def Function: Rule1[PathFunction] =
    functionMin | functionMax | functionAvg | functionLength | functionSum | functionFirst | functionLast | functionIndex

  private def functionMin: Rule1[PathFunction] = rule { ".min()" ~ push(PathFunction.MinFunction) }
  private def functionMax: Rule1[PathFunction] = rule { ".max()" ~ push(PathFunction.MaxFunction) }
  private def functionAvg: Rule1[PathFunction] = rule { ".avg()" ~ push(PathFunction.AvgFunction) }
  private def functionLength: Rule1[PathFunction] = rule { ".length()" ~ push(PathFunction.LengthFunction) }
  private def functionSum: Rule1[PathFunction] = rule { ".sum()" ~ push(PathFunction.SumFunction) }
  private def functionFirst: Rule1[PathFunction] = rule { ".first()" ~ push(PathFunction.FirstFunction) }
  private def functionLast: Rule1[PathFunction] = rule { ".last()" ~ push(PathFunction.LastFunction) }
  private def functionIndex: Rule1[PathFunction] = rule { ".index(" ~ int ~ ")" ~~> PathFunction.IndexFunction.apply }

  /*
   * from jayway's impl:
   *
   *  LogicalOR               = LogicalAND { '||' LogicalAND }
   *  LogicalAND              = LogicalANDOperand { '&&' LogicalANDOperand }
   *  LogicalANDOperand       = RelationalExpression | '(' LogicalOR ')' | '!' LogicalANDOperand
   *  RelationalExpression    = Value [ RelationalOperator Value ]
   */

  // filter op on a path
  def FilterOp: Rule1[PathFilter] = rule { "[?(" ~ whitespace ~ logicalOr ~ whitespace ~ ")]" ~~> PathFilter.apply  }

  private def logicalOr: Rule1[FilterOp] = rule {
    oneOrMore(logicalAnd, whitespace ~ "||" ~ whitespace) ~~> { ops =>
      if (ops.lengthCompare(1) == 0) { ops.head } else { OrFilterOp(ops) }
    }
  }
  private def logicalAnd: Rule1[FilterOp] = rule {
    oneOrMore(andOperand, whitespace ~ "&&" ~ whitespace) ~~> { ops =>
      if (ops.lengthCompare(1) == 0) { ops.head } else { AndFilterOp(ops) }
    }
  }
  private def andOperand: Rule1[FilterOp] = rule { filter | "(" ~ whitespace ~ logicalOr ~ whitespace ~ ")" | logicalNot }
  private def logicalNot: Rule1[FilterOp] = rule { "!" ~ whitespace ~ andOperand ~~> NotFilterOp.apply  }

  private def filter: Rule1[FilterOp] =
    equalsFilter | notEqualsFilter | lessThanOrEqualsFilter | lessThanFilter | greaterThanOrEqualsFilter |
      greaterThanFilter | inFilter | ninFilter | subsetFilter | anyOfFilter | noneOfFilter | sizeFilter |
      regexFilter | isEmptyFilter | existsFilter

  private def equalsFilter: Rule1[EqualsOp] = rule { binaryFilter("==", EqualsOp.apply) }
  private def notEqualsFilter: Rule1[NotEqualsOp] = rule { binaryFilter("!=", NotEqualsOp.apply) }
  private def lessThanOrEqualsFilter: Rule1[LessThanOrEqualsOp] = rule { binaryFilter("<=", LessThanOrEqualsOp.apply) }
  private def lessThanFilter: Rule1[LessThanOp] = rule { binaryFilter("<", LessThanOp.apply) }
  private def greaterThanOrEqualsFilter: Rule1[GreaterThanOrEqualsOp] = rule { binaryFilter(">=", GreaterThanOrEqualsOp.apply) }
  private def greaterThanFilter: Rule1[GreaterThanOp] = rule { binaryFilter(">", GreaterThanOp.apply) }
  private def inFilter: Rule1[InOp] = rule { binaryStringOpFilter("in", InOp.apply) }
  private def ninFilter: Rule1[NotInOp] = rule { binaryStringOpFilter("nin", NotInOp.apply) }
  private def subsetFilter: Rule1[IsSubsetOp] = rule { binaryStringOpFilter("subsetof", IsSubsetOp.apply) }
  private def anyOfFilter: Rule1[AnyOfOp] = rule { binaryStringOpFilter("anyof", AnyOfOp.apply) }
  private def noneOfFilter: Rule1[NoneOfOp] = rule { binaryStringOpFilter("noneof", NoneOfOp.apply) }
  private def sizeFilter: Rule1[EqualSizeOp] = rule { binaryStringOpFilter("size", EqualSizeOp.apply) }
  private def isEmptyFilter: Rule1[IsEmptyOp] = rule { binaryStringOpFilter("empty", IsEmptyOp.apply) }

  private def binaryFilter[T](op: String, builder: (FilterExpression, FilterExpression) => T) =
    (filterExpression ~ whitespace ~ op ~ whitespace ~ filterExpression) ~~> { (left, right) => builder(left, right) }
  private def binaryStringOpFilter[T](op: String, builder: (FilterExpression, FilterExpression) => T) =
    (filterExpression ~ space ~ op ~ space ~ filterExpression) ~~> { (left, right) => builder(left, right) }

  private def regexFilter: Rule1[RegexOp] = rule {
    (pathFilterExpression ~ whitespace ~ "=~" ~ whitespace ~ "/" ~ regexPattern ~ "/" ~ optional("i" ~ push(true))) ~~> {
      (left, pattern, i) => RegexOp(left, Pattern.compile(pattern, if (i.contains(true)) { Pattern.CASE_INSENSITIVE } else { 0 }))
    }
  }
  private def regexPattern: Rule1[String] = rule {
    oneOrMore((noneOf("/") ~? notControlChar) | escapedChar) ~> StringEscapeUtils.unescapeJava
  }

  private def existsFilter: Rule1[ExistsOp] = rule { filterExpression ~~> ExistsOp.apply }

  // an argument in a filter - a path or literal
  private def filterExpression: Rule1[FilterExpression] =
    pathFilterExpression | literalFilterString | literalFilterNumber | literalFilterBoolean |
      literalFilterStringArray | literalFilterNumberArray | literalFilterBooleanArray

  private def pathFilterExpression: Rule1[PathExpression] = rule {
    ("$"  ~ push(true) | "@" ~ push(false)) ~ zeroOrMore(Element) ~ optional(Function) ~~> { (absolute, e, f) =>
      PathExpression(JsonPath(e, f), absolute = absolute)
    }
  }

  private def literalFilterNumber: Rule1[NumericLiteral] =
    (double | float | long | int) ~~> (n => NumericLiteral(BigDecimal(n.toString)))
  private def literalFilterString: Rule1[StringLiteral] = (quotedString | singleQuotedString) ~~> StringLiteral.apply
  private def literalFilterBoolean: Rule1[BooleanLiteral] = boolean ~~> BooleanLiteral.apply
  private def literalFilterNumberArray: Rule1[ArrayLiteral[NumericLiteral]] =
    "[" ~ whitespace ~ oneOrMore(literalFilterNumber, "," ~ whitespace) ~ whitespace ~ "]" ~~> ArrayLiteral.apply
  private def literalFilterStringArray: Rule1[ArrayLiteral[StringLiteral]] =
    "[" ~ whitespace ~ oneOrMore(literalFilterString, "," ~ whitespace) ~ whitespace ~ "]" ~~> ArrayLiteral.apply
  private def literalFilterBooleanArray: Rule1[ArrayLiteral[BooleanLiteral]] =
    "[" ~ whitespace ~ oneOrMore(literalFilterBoolean, "," ~ whitespace) ~ whitespace ~ "]" ~~> ArrayLiteral.apply
}
