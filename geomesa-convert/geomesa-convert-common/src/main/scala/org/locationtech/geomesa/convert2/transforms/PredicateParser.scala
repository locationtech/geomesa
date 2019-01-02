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
    and | or | not | comparison
  }

  private def and: Rule1[Predicate] = rule("and") {
    ("and(" ~ pred ~ "," ~ whitespace ~ pred ~ ")") ~~> { (left, right) => And(left, right) }
  }

  private def or: Rule1[Predicate] = rule("or") {
    ("or(" ~ pred ~ "," ~ whitespace ~ pred ~ ")") ~~> { (left, right) => Or(left, right) }
  }

  private def not: Rule1[Predicate] = rule("not") {
    ("not(" ~ pred ~ ")") ~~> { (p) => Not(p) }
  }

  private def comparison: Rule1[Predicate] = rule {
    stringCompare | intCompare | longCompare | floatCompare | doubleCompare | booleanCompare
  }

  private def stringCompare: Rule1[Predicate] = rule {
    ("str" ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => binaryComparison[String](comp, left, right)
    }
  }

  private def intCompare: Rule1[Predicate] = rule {
    (("integer" | "int") ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => binaryComparison[Integer](comp, left, right)
    }
  }

  private def longCompare: Rule1[Predicate] = rule {
    ("long" ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => binaryComparison[java.lang.Long](comp, left, right)
    }
  }

  private def floatCompare: Rule1[Predicate] = rule {
    ("float" ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => binaryComparison[java.lang.Float](comp, left, right)
    }
  }

  private def doubleCompare: Rule1[Predicate] = rule {
    ("double" ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => binaryComparison[java.lang.Double](comp, left, right)
    }
  }

  private def booleanCompare: Rule1[Predicate] = rule {
    (("boolean" | "bool") ~ comparisonType ~ "(" ~ expr ~ "," ~ whitespace ~ expr ~ ")") ~~> {
      (comp, left, right) => comp match {
        case Eq  => BinaryEquals[java.lang.Boolean](left, right)
        case NEq => BinaryNotEquals[java.lang.Boolean](left, right)
        case _ => throw new ParsingException(s"Invalid boolean comparison operator: $comp")
      }
    }
  }

  private def comparisonType: Rule1[Comparisons] = rule {
    (Eq.toString | LTEq.toString | GTEq.toString | LT.toString | GT.toString | NEq.toString ) ~> {
      (s) => Comparisons.withName(s)
    }
  }

  private def binaryComparison[T](comp: Comparisons,
                                  left: Expression,
                                  right: Expression)
                                 (implicit ordering: Ordering[T]): Predicate = {
    comp match {
      case Eq   => BinaryEquals[T](left, right)
      case LT   => BinaryLessThan[T](left, right)
      case GT   => BinaryGreaterThan[T](left, right)
      case LTEq => BinaryLessThanOrEquals[T](left, right)
      case GTEq => BinaryGreaterThanOrEquals[T](left, right)
      case NEq  => BinaryNotEquals[T](left, right)
    }
  }
}
