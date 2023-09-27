/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import org.locationtech.geomesa.index.conf.splitter.SplitPatternParser._
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.utils.text.{BasicParser, DateParsing}
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

import java.util.Date

/**
  * Parses patterns into splits
  */
object SplitPatternParser {

  private val Parser = new SplitPatternParser()

  @throws(classOf[ParsingException])
  def parse(pattern: String): SplitPattern = parse(pattern, report = true)

  @throws(classOf[ParsingException])
  def parse(pattern: String, report: Boolean): SplitPattern = {
    if (pattern == null) {
      throw new IllegalArgumentException("Invalid split pattern: null")
    }

    val runner = if (report) { ReportingParseRunner(Parser.patterns) } else { BasicParseRunner(Parser.patterns) }
    val parsing = runner.run(pattern.stripMargin('|').replaceAll("\\s*", ""))

    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid split pattern: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  sealed trait SplitPattern {
    def range: Seq[String]
    private [SplitPatternParser] def reverse: Seq[String] = range.reverse
  }

  case class AlphaPattern(start: Char, end: Char) extends SplitPattern {
    override def range: Seq[String] =
      Seq.range(math.min(start, end), math.max(start, end) + 1).map(_.toChar.toString)
  }

  case class NumericPattern(start: Byte, end: Byte) extends SplitPattern {
    override def range: Seq[String] =
      Seq.range(math.min(start, end), math.max(start, end) + 1).map(_.toString)
  }

  case class CompositePattern(patterns: Seq[SplitPattern]) extends SplitPattern {
    override def range: Seq[String] = patterns.flatMap(_.range)
    override private [SplitPatternParser] def reverse: Seq[String] = patterns.flatMap(_.reverse)
  }

  case class TieredPatterns(tiers: Seq[SplitPattern]) extends SplitPattern {
    override def range: Seq[String] =
      tiers.map(_.range).reduceLeft { (left, right) => for (a <- left; b <- right) yield { a + b } }
  }

  case class NegativePatterns(tiers: Seq[SplitPattern]) extends SplitPattern {
    override def range: Seq[String] =
      tiers.map(_.reverse).foldLeft(Seq("-")) { (left, right) => for (a <- left; b <- right) yield { a + b } }
  }

  case class DatePattern(from: Date, to: Date, ranges: Int) extends SplitPattern {
    override def range: Seq[String] = {
      val interval = (to.getTime - from.getTime) / ranges
      Seq.tabulate(ranges) { i =>
        AttributeIndexKey.encodeForQuery(new Date(from.getTime + interval * i), classOf[Date])
      }
    }
  }
}

private class SplitPatternParser extends BasicParser {

  // Valid patterns look like the following:

  // [a-z][0-9]
  // [0-35-8][a-z]
  // [f][0-9]

  import org.parboiled.scala._

  def patterns: Rule1[SplitPattern] = rule {
    (mixedPatterns | negativePatterns | dateRange) ~ EOI
  }

  private def mixedPatterns: Rule1[SplitPattern] = rule {
    oneOrMore(mixedTier) ~~> {
      case tiers if tiers.lengthCompare(1) == 0 => tiers.head
      case tiers => TieredPatterns(tiers)
    }
  }

  private def negativePatterns: Rule1[SplitPattern] = rule {
    "[-]" ~ oneOrMore(numericTier) ~~> { tiers => NegativePatterns(tiers) }
  }

  private def mixedTier: Rule1[SplitPattern] = rule {
    "[" ~ { oneOrMore(alpha | numeric) ~~> { p => if (p.length == 1) { p.head } else { CompositePattern(p) } } } ~ "]"
  }

  private def numericTier: Rule1[SplitPattern] = rule {
    "[" ~ { oneOrMore(numeric) ~~> { p => if (p.length == 1) { p.head } else { CompositePattern(p) } } } ~ "]"
  }

  private def dateRange: Rule1[SplitPattern] = rule {
    (date ~ "/" ~ date ~ "/" ~ int) ~~> { (from, to, splits) => DatePattern(from, to, splits) }
  }

  private def alpha: Rule1[AlphaPattern] = rule {
    alphaRange | alphaSingle
  }

  private def numeric: Rule1[NumericPattern] = rule {
    numericRange | numericSingle
  }

  private def alphaSingle: Rule1[AlphaPattern] = rule {
    alphaEndpoint ~~> { e => AlphaPattern(e, e) }
  }

  private def numericSingle: Rule1[NumericPattern] = rule {
    numericEndpoint ~~> { e => NumericPattern(e, e) }
  }

  private def alphaRange: Rule1[AlphaPattern] = rule {
    (alphaEndpoint ~ "-" ~ alphaEndpoint) ~~> { (from, to) => AlphaPattern(from, to) }
  }

  private def numericRange: Rule1[NumericPattern] = rule {
    (numericEndpoint ~ "-" ~ numericEndpoint) ~~> { (from, to) => NumericPattern(from, to) }
  }

  private def alphaEndpoint: Rule1[Char] = rule { ("a" - "z" | "A" - "Z") ~> { c => c.charAt(0) } }

  private def numericEndpoint: Rule1[Byte] = rule { ("0" - "9") ~> { c => c.toByte } }

  private def date: Rule1[Date] = rule {
    group(year ~ "-" ~ month ~ "-" ~ day) ~> { date => DateParsing.parseDate(date) }
  }

  private def year: Rule0 = rule { nTimes(4, "0" - "9")  }

<<<<<<< HEAD
  private def month: Rule0 = rule { ("0" ~ ("1" - "9")) | ("1" ~ ("0" - "2")) }

  private def day: Rule0 = rule { (("0" - "2") ~ ("0" - "9")) | ("3" ~ ("0" - "1")) }
=======
  private def month: Rule0 = rule { ("0" - "1") ~ ("1" - "9") }

  private def day: Rule0 = rule { ("0" - "3") ~ ("0" - "9") }
>>>>>>> bc50eb4be1 (GEOMESA-3297 Support for tiered date index pre-splits (#2996))
}
