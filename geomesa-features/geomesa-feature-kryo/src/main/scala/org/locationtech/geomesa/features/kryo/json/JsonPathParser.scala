/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.locationtech.geomesa.features.kryo.json.JsonPathParser.JsonPathFunction.JsonPathFunction
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.parboiled.Context
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala._

/**
  * Parses a json path string into a sequence of selectors. See https://github.com/jayway/JsonPath for examples.
  *
  * Does not support filter predicates.
  */
object JsonPathParser {

  private val Parser = new JsonPathParser()

  @throws(classOf[ParsingException])
  def parse(path: String, report: Boolean = true): Seq[PathElement] = {
    val runner = if (report) { ReportingParseRunner(Parser.Path) } else { BasicParseRunner(Parser.Path) }
    val parsing = runner.run(path)
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid json path: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  sealed trait PathElement

  // attribute: .foo
  case class PathAttribute(name: String) extends PathElement

  // enumerated index: [1]
  case class PathIndex(index: Int) extends PathElement

  // enumerated indices: [1,2,5]
  case class PathIndices(indices: Seq[Int]) extends PathElement

  // any attribute: .*
  case object PathAttributeWildCard extends PathElement

  // any index: [*]
  case object PathIndexWildCard extends PathElement

  // deep scan: ..
  case object PathDeepScan extends PathElement

  // path function: .min(), .max(), .avg(), .length()
  // not implemented: stddev
  case class PathFunction(function: JsonPathFunction) extends PathElement

  object JsonPathFunction extends Enumeration {
    type JsonPathFunction = Value
    val min, max, avg, length = Value
  }
}

private class JsonPathParser extends Parser {

  // main parsing rule
  def Path: Rule1[Seq[PathElement]] =
  rule { "$" ~ zeroOrMore(Element) ~ optional(Function) ~~> ((e, f) => e ++ f.toSeq) ~ EOI }

  def Element: Rule1[PathElement] = rule {
    Attribute | ArrayIndex | ArrayIndices | ArrayIndexRange | AttributeWildCard | IndexWildCard | DeepScan
  }

  def IndexWildCard: Rule1[PathElement] = rule { "[*]" ~ push(PathIndexWildCard) }

  def AttributeWildCard: Rule1[PathElement] = rule { ".*" ~ push(PathAttributeWildCard) }

  // we have to push the deep scan directly onto the stack as there is no forward matching and
  // it's ridiculous trying to combine Rule1's and Rule2's
  def DeepScan: Rule1[PathElement] = rule { "." ~ toRunAction(pushDeepScan) ~ (Attribute | AttributeWildCard) }

  // note: this assumes that we are inside a zeroOrMore, which is currently the case
  // the zeroOrMore will have pushed a single list onto the value stack - we append our value to that
  private def pushDeepScan(context: Context[Any]): Unit =
    context.getValueStack.push(PathDeepScan :: context.getValueStack.pop.asInstanceOf[List[_]])

  def ArrayIndex: Rule1[PathIndex] = rule { "[" ~ Number ~ "]" ~~> PathIndex }

  def ArrayIndices: Rule1[PathIndices] =
    rule { "[" ~ Number ~ "," ~ oneOrMore(Number, ",") ~ "]" ~~> ((n0, n) => PathIndices(n.+:(n0))) }

  def ArrayIndexRange: Rule1[PathIndices] =
    rule { "[" ~ Number ~ ":" ~ Number ~ "]" ~~> ((n0, n1) => PathIndices(n0 until n1)) }

  def Attribute: Rule1[PathAttribute] = rule { "." ~ oneOrMore(Character) ~> PathAttribute ~ !"()" }

  def Function: Rule1[PathFunction] = rule {
    "." ~ ("min" | "max" | "avg" | "length") ~> ((f) => PathFunction(JsonPathFunction.withName(f))) ~ "()"
  }

  def Number: Rule1[Int] = rule { oneOrMore("0" - "9") ~> (_.toInt) }

  def Character: Rule0 = rule { EscapedChar | NormalChar }

  def EscapedChar: Rule0 = rule { "\\" ~ (anyOf("\"\\/bfnrt") | Unicode) }

  def NormalChar: Rule0 = rule { "a" - "z" | "A" - "Z" | "0" - "9" }

  def Unicode: Rule0 = rule { "u" ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit }

  def HexDigit: Rule0 = rule { "0" - "9" | "a" - "f" | "A" - "F" }
}
