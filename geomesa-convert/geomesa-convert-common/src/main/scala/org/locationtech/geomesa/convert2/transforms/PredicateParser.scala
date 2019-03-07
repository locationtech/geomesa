/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.convert2.transforms.Predicate._
import org.locationtech.geomesa.convert2.transforms.PredicateParser.Comparisons
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

object PredicateParser extends StrictLogging {

  private val Parser = new PredicateParser()

  @throws(classOf[ParsingException])
  def parse(predicate: String, report: Boolean = true): Predicate = {
    logger.trace(s"Parsing predicate: $predicate")
    if (predicate == null) {
      throw new IllegalArgumentException("Invalid predicate: null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.predicate) } else { BasicParseRunner(Parser.predicate) }
    val parsing = runner.run(predicate)
    parsing.result.getOrElse(throw new ParsingException(s"Invalid predicate: ${ErrorUtils.printParseErrors(parsing)}"))
  }

  private object Comparisons extends Enumeration {
    type Comparisons = Value
    val Eq, LT, GT, LTEq, GTEq, NEq  = Value
  }
}

private class PredicateParser extends ExpressionParser {

  import PredicateParser.Comparisons._
  import org.parboiled.scala._

  // full predicate
  def predicate: Rule1[Predicate] = rule("predicate") { pred ~ EOI }

  private def pred: Rule1[Predicate] = rule("pred") {
    whitespace ~ (and | or | grouped) ~ whitespace
  }

  private def grouped: Rule1[Predicate] = rule("simple") {
    whitespace ~ (andFn | orFn | not | comparison | ("(" ~ and ~ ")") | ("(" ~ or ~ ")")) ~ whitespace
  }

  private def andFn: Rule1[Predicate] = rule("andFn") {
    ("and(" ~ pred ~ "," ~ oneOrMore(pred, ",") ~ ")") ~~> { (head, tail) => And(head, tail) }
  }

  private def and: Rule1[Predicate] = rule("and") {
    (grouped ~ "&&" ~ oneOrMore(grouped, "&&")) ~~> { (head, tail) => And(head, tail) }
  }

  private def orFn: Rule1[Predicate] = rule("orFn") {
    ("or(" ~ pred ~ "," ~ oneOrMore(pred, ",") ~ ")") ~~> { (head, tail) => Or(head, tail) }
  }

  private def or: Rule1[Predicate] = rule("or") {
    (grouped ~ "||" ~ oneOrMore(grouped, "||")) ~~> { (head, tail) => Or(head, tail) }
  }

  private def not: Rule1[Predicate] = rule("not") {
    (("not(" | "!(") ~ pred ~ ")") ~~> { p => Not(p) }
  }

  private def comparison: Rule1[Predicate] = rule {
    compareFn | booleanCompare | compare
  }

  private def compare: Rule1[Predicate] = rule {
    (expr ~ whitespace ~ op ~ whitespace ~ expr) ~~> {
      (left, comp, right) => comp match {
        case "==" => BinaryEquals(left, right)
        case "!=" => BinaryNotEquals(left, right)
        case "<"  => BinaryLessThan(left, right)
        case "<=" => BinaryLessThanOrEquals(left, right)
        case ">"  => BinaryGreaterThan(left, right)
        case ">=" => BinaryGreaterThanOrEquals(left, right)
      }
    }
  }

  private def op: Rule1[String] = rule {
    ("==" | "!=" | "<=" | "<"  | ">=" | ">") ~> { s => s}
  }

  private def compareFn: Rule1[Predicate] = rule {
    (compareFnName ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => comp match {
        case Eq   => BinaryEquals(left, right)
        case LT   => BinaryLessThan(left, right)
        case GT   => BinaryGreaterThan(left, right)
        case LTEq => BinaryLessThanOrEquals(left, right)
        case GTEq => BinaryGreaterThanOrEquals(left, right)
        case NEq  => BinaryNotEquals(left, right)
      }
    }
  }

  private def compareFnName: Rule0 = rule {
    "str" | "integer" | "int" | "long" | "float" | "double"
  }

  private def booleanCompare: Rule1[Predicate] = rule {
    (("boolean" | "bool") ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => comp match {
        case Eq  => BinaryEquals(left, right)
        case NEq => BinaryNotEquals(left, right)
        case _ => throw new ParsingException(s"Invalid boolean comparison operator: $comp")
      }
    }
  }

  private def comparisonType: Rule1[Comparisons] = rule {
    (Eq.toString | LTEq.toString | GTEq.toString | LT.toString | GT.toString | NEq.toString) ~> {
      s => Comparisons.withName(s)
    }
  }
}
