/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.util.regex.Pattern

import org.apache.commons.text.StringEscapeUtils
import org.parboiled.scala._
import org.parboiled.scala.rules.Rule1

/**
  * Base class for parboiled parsers that provides methods for string and number matching
  */
class BasicParser extends Parser {

  private val controlCharPattern = Pattern.compile("""\p{Cntrl}""")

  def int: Rule1[Int] = rule { group(optional("-") ~ oneOrMore("0" - "9")) ~> (_.toInt) }

  def long: Rule1[Long] = rule { group(optional("-") ~ oneOrMore("0" - "9")) ~> (_.toLong) ~ "L" }

  def float: Rule1[Float] = rule {
    group(optional("-") ~ zeroOrMore("0" - "9") ~ "." ~ oneOrMore("0" - "9")) ~> (_.toFloat) ~ ("f" | "F")
  }

  def double: Rule1[Double] = rule {
    group(optional("-") ~ zeroOrMore("0" - "9") ~ "." ~ oneOrMore("0" - "9")) ~> (_.toDouble) ~ optional("d" | "D")
  }

  def boolean: Rule1[Boolean] = rule { (ignoreCase("true") | ignoreCase("false")) ~> (_.toBoolean) }

  def char: Rule0 = rule { "a" - "z" | "A" - "Z" | "0" - "9" | "_" }

  def string: Rule1[String] = rule { quotedString | singleQuotedString | unquotedString }

  def unquotedString: Rule1[String] = rule { oneOrMore(char) ~> { c => c } }

  def quotedString: Rule1[String] = rule {
    "\"" ~ zeroOrMore((noneOf("""\"""") ~? notControlChar) | escapedChar) ~> StringEscapeUtils.unescapeJava ~ "\""
  }

  def singleQuotedString: Rule1[String] = rule {
    "'" ~ zeroOrMore((noneOf("""\'""") ~? notControlChar) | escapedChar) ~> StringEscapeUtils.unescapeJava ~ "'"
  }

  def whitespace: Rule0 = rule { zeroOrMore(anyOf(" \n\r\t\f")) }

  private def escapedChar: Rule0 = rule {
    "\\" ~ (anyOf("""\/"'bfnrt""") | "u" ~ nTimes(4, "0" - "9" | "a" - "f" | "A" - "F"))
  }

  private def notControlChar(s: String): Boolean = !controlCharPattern.matcher(s).matches()
}
