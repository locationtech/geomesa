/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

/**
  * Parses patterns into splits
  */
object SplitPatternParser {

  private val Parser = new SplitPatternParser()

  @throws(classOf[ParsingException])
  def parse(pattern: String): Seq[(String, String)] = parse(pattern, report = true)

  @throws(classOf[ParsingException])
  def parse(pattern: String, report: Boolean): Seq[(String, String)] = {
    if (pattern == null) {
      throw new IllegalArgumentException("Invalid split pattern: null")
    }

    val runner = if (report) { ReportingParseRunner(Parser.patterns) } else { BasicParseRunner(Parser.patterns) }
    val parsing = runner.run(pattern.stripMargin('|').replaceAll("\\s*", ""))

    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid split pattern: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }
}

private class SplitPatternParser extends BasicParser {

  // Valid patterns look like the following:

  // [a-z][0-9]
  // [0-35-8][a-z]
  // [f][0-9]

  import org.parboiled.scala._

  def patterns: Rule1[Seq[(String, String)]] = rule {
    oneOrMore(level) ~ EOI ~~> { (levels) =>
      levels.reduceLeft { (left: Seq[(String, String)], right: Seq[(String, String)]) =>
        for (a <- left; b <- right) yield { (a._1 + b._1, a._2 + b._2) }
      }
    }
  }

  private def level: Rule1[Seq[(String, String)]] = rule {
    "[" ~ oneOrMore(range | single) ~ "]"
  }

  private def single: Rule1[(String, String)] = rule {
    endpoint ~~> { (value) => (value, value) }
  }

  private def range: Rule1[(String, String)] = rule {
    (endpoint ~ "-" ~ endpoint) ~~> { (from, to) => (from, to) }
  }

  private def endpoint: Rule1[String] = rule { char ~> { (c) => c } }
}
