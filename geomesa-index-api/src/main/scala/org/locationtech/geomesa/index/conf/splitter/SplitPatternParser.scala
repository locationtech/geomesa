/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import com.google.common.primitives.Bytes
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

/**
  * Parses patterns into splits
  */
object SplitPatternParser {

  private val Parser = new SplitPatternParser()

  @throws(classOf[ParsingException])
  def parse(pattern: String): Array[Array[Byte]] = parse(pattern, report = true)

  @throws(classOf[ParsingException])
  def parse(pattern: String, report: Boolean): Array[Array[Byte]] = {
    if (pattern == null) {
      throw new IllegalArgumentException("Invalid split pattern: null")
    }

    val runner = if (report) { ReportingParseRunner(Parser.patterns) } else { BasicParseRunner(Parser.patterns) }
    val parsing = runner.run(pattern.stripMargin('|').replaceAll("\\s*", ""))

    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid split pattern: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  /**
    * Splits from one char to a second (inclusive)
    *
    * @param from from
    * @param to to
    * @return
    */
  private def rangeSplits(from: Char, to: Char): Array[Array[Byte]] =
    Array.range(from, to + 1).map(b => Array(b.toByte))
}

private class SplitPatternParser extends BasicParser {

  // Valid patterns look like the following:

  // [a-z][0-9]
  // [0-35-8][a-z]
  // [f][0-9]

  import SplitPatternParser.rangeSplits
  import org.parboiled.scala._

  def patterns: Rule1[Array[Array[Byte]]] = rule {
    oneOrMore(level) ~ EOI ~~> {
      (levels) => levels.reduceLeft((left, right) => for (a <- left; b <- right) yield { Bytes.concat(a, b) })
    }
  }

  private def level: Rule1[Array[Array[Byte]]] = rule {
    "[" ~ oneOrMore(range | single) ~ "]" ~~> { _.reduceLeft(_ ++ _) }
  }

  private def single: Rule1[Array[Array[Byte]]] = rule {
    endpoint ~~> { (value) => rangeSplits(value, value) }
  }

  private def range: Rule1[Array[Array[Byte]]] = rule {
    (endpoint ~ "-" ~ endpoint) ~~> { (from, to) => rangeSplits(from, to) }
  }

  private def endpoint: Rule1[Char] = rule { char ~> { _.charAt(0) } }
}
