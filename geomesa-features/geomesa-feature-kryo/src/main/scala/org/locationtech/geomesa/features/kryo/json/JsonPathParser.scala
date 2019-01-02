/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo.json

import org.locationtech.geomesa.features.kryo.json.JsonPathParser.JsonPathFunction.JsonPathFunction
import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.locationtech.geomesa.utils.text.BasicParser
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

  def print(path: Seq[PathElement], dollar: Boolean = true): String = {
    require(path.nonEmpty, "Path must be non-empty")
    if (dollar) {
      path.mkString("$", "", "")
    } else {
      val string = path.mkString
      if (string.charAt(0) == '.') {
        // trim off leading self-selector to correspond to dot notation selection
        string.substring(1)
      } else {
        string
      }
    }
  }

  sealed trait PathElement

  // attribute: .foo or ['foo']
  case class PathAttribute(name: String, bracketed: Boolean = false) extends PathElement {
    override def toString: String = if (bracketed) { s"['$name']" } else { s".$name" }
  }

  // enumerated index: [1]
  case class PathIndex(index: Int) extends PathElement {
    override def toString: String = s"[$index]"
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

  // path function: .min(), .max(), .avg(), .length()
  // not implemented: stddev
  case class PathFunction(function: JsonPathFunction) extends PathElement {
    override def toString: String = s".$function()"
  }

  object JsonPathFunction extends Enumeration {
    type JsonPathFunction = Value
    val min, max, avg, length = Value
  }
}

private class JsonPathParser extends BasicParser {

  // main parsing rule
  def Path: Rule1[Seq[PathElement]] =
    rule { "$" ~ zeroOrMore(Element) ~ optional(Function) ~~> ((e, f) => e ++ f.toSeq) ~ EOI }

  def Element: Rule1[PathElement] = rule {
    Attribute | ArrayIndex | ArrayIndices | ArrayIndexRange | BracketedAttribute | AttributeWildCard | IndexWildCard | DeepScan
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

  def ArrayIndex: Rule1[PathIndex] = rule { "[" ~ int ~ "]" ~~> PathIndex }

  def ArrayIndices: Rule1[PathIndices] =
    rule { "[" ~ int ~ "," ~ oneOrMore(int, ",") ~ "]" ~~> ((n0, n) => PathIndices(n.+:(n0))) }

  def ArrayIndexRange: Rule1[PathIndices] =
    rule { "[" ~ int ~ ":" ~ int ~ "]" ~~> ((n0, n1) => PathIndices(n0 until n1)) }

  def Attribute: Rule1[PathAttribute] = rule { "." ~ oneOrMore(char) ~> { (s) => PathAttribute(s) } ~ !"()" }

  def BracketedAttribute: Rule1[PathAttribute] =
    rule { "[" ~ bracketedString ~~> { (s) => PathAttribute(s, bracketed = true) } ~ "]" }

  private def bracketedString: Rule1[String] = unquotedString | singleQuotedString

  def Function: Rule1[PathFunction] = rule {
    "." ~ ("min" | "max" | "avg" | "length") ~> ((f) => PathFunction(JsonPathFunction.withName(f))) ~ "()"
  }
}
