/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

/**
  * Parses a simple set of key-value pairs, in the form 'key1:value1,key2:value2'
  */
object KVPairParser {

  private val Parser = new KVPairParser()

  @throws(classOf[ParsingException])
  def parse(s: String): Map[String, String] = parse(s, report = true)

  @throws(classOf[ParsingException])
  def parse(s: String, report: Boolean): Map[String, String] = {
    val runner = if (report) { ReportingParseRunner(Parser.map) } else { BasicParseRunner(Parser.map) }
    val parsing = runner.run(s.stripMargin('|').replaceAll("\\s*", ""))

    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid split pattern: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }
}

private class KVPairParser(pairSep: String = ",", kvSep: String = ":") extends BasicParser {

  import org.parboiled.scala._

  private def key: Rule1[String] = rule {
    oneOrMore(char | anyOf(".-")) ~> { (k) => k }
  }

  private def value: Rule1[String] = rule {
    quotedString | singleQuotedString | oneOrMore(char | anyOf(".-[]%")) ~> { (k) => k }
  }

  private def keyValue: Rule1[(String, String)] = rule {
    (key ~ kvSep ~ value) ~~> { (k, v) => (k, v) }
  }

  def map: Rule1[Map[String, String]] = rule {
    oneOrMore(keyValue, pairSep) ~ EOI ~~> { (kvs) => kvs.toMap }
  }
}