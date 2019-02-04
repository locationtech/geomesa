/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.Expression._
import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

/**
  * Parser for converter transforms
  */
object ExpressionParser extends StrictLogging {

  private val Parser = new ExpressionParser()

  @throws(classOf[ParsingException])
  def parse(expression: String, report: Boolean = true): Expression = {
    logger.trace(s"Parsing expression: $expression")
    if (expression == null) {
      throw new IllegalArgumentException("Invalid expression: null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.expression) } else { BasicParseRunner(Parser.expression) }
    val parsing = runner.run(expression)
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid expression: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }
}

private [transforms] class ExpressionParser extends BasicParser {

  import org.parboiled.scala._

  implicit private val ec: EvaluationContext = EvaluationContext.empty

  // full expression
  def expression: Rule1[Expression] = rule("expression") { expr ~ EOI }

  protected def expr: Rule1[Expression] = rule("expr") {
    whitespace ~ (cast | nonCast) ~ whitespace
  }

  private def cast: Rule1[Expression] = rule("cast") {
    castToInt | castToLong | castToFloat | castToDouble | castToString | castToBoolean | castToRegex
  }

  private def nonCast: Rule1[Expression] = rule {
    tryFunction | function | column | field | literal
  }

  private def literal: Rule1[Expression] = rule("literal") {
    // order is important - most to least specific
    litFloat | litDouble | litLong | litInt | litBoolean | litString | litNull
  }

  private def column: Rule1[Expression] = rule("$col") {
    "$" ~ int ~~> { i => Column(i) }
  }

  private def field: Rule1[Expression] = rule("$field") {
    "$" ~ unquotedString ~~> { i => FieldLookup(i) }
  }

  private def tryFunction: Rule1[Expression] = rule("try") {
    ("try" ~ whitespace ~ "(" ~ expr ~ "," ~ expr ~ ")") ~~> { (primary, fallback) => TryExpression(primary, fallback) }
  }

  private def function: Rule1[Expression] = rule("function") {
    (optional(unquotedString ~ ":") ~ unquotedString ~ whitespace ~ "(" ~ zeroOrMore(expr, ",") ~ ")") ~~> {
      (ns, fn, args) => {
        val name = ns.map(_ + ":" + fn).getOrElse(fn)
        val function = TransformerFunction.functions.getOrElse(name,
          throw new ParsingException(s"Invalid function name: $name"))
        val expr = FunctionExpression(function.getInstance, args.toArray)
        if (function.pure && args.forall(a => a.isInstanceOf[Literal[_]])) {
          LiteralAny(expr.eval(Array.empty))
        } else {
          expr
        }
      }
    }
  }

  private def litInt: Rule1[Expression] = rule("int") {
    int ~~> { e => LiteralInt(e) }
  }

  private def litLong: Rule1[Expression] = rule("long") {
    long ~~> { e => LiteralLong(e) }
  }

  private def litFloat: Rule1[Expression] = rule("float") {
    float ~~> { e => LiteralFloat(e) }
  }

  private def litDouble: Rule1[Expression] = rule("double") {
    double ~~> { e => LiteralDouble(e) }
  }

  private def litBoolean: Rule1[Expression] = rule("boolean") {
    boolean ~~> { e => LiteralBoolean(e) }
  }

  private def litString: Rule1[Expression] = rule("string") {
    (quotedString | singleQuotedString) ~~> { e => LiteralString(e) }
  }

  private def litNull: Rule1[Expression] = rule("null") {
    "null" ~> { _ => LiteralNull }
  }

  private def castToInt: Rule1[Expression] = rule("::int") {
    (nonCast ~ ("::integer" | "::int")) ~~> {
      e => {
        val expr = CastToInt(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralInt(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToLong: Rule1[Expression] = rule("::long") {
    (nonCast ~ "::long") ~~> {
      e => {
        val expr = CastToLong(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralLong(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToFloat: Rule1[Expression] = rule("::float") {
    (nonCast ~ "::float") ~~> {
      e => {
        val expr = CastToFloat(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralFloat(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToDouble: Rule1[Expression] = rule("::double") {
    (nonCast ~ "::double") ~~> {
      e => {
        val expr = CastToDouble(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralDouble(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToString: Rule1[Expression] = rule("::string") {
    (nonCast ~ "::string") ~~> {
      e => {
        val expr = CastToString(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralString(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToBoolean: Rule1[Expression] = rule("::boolean") {
    (nonCast ~ "::" ~ ("boolean" | "bool")) ~~> {
      e => {
        val expr = CastToBoolean(e)
        if (e.isInstanceOf[Literal[_]]) { LiteralBoolean(expr.eval(Array.empty)) } else { expr }
      }
    }
  }

  private def castToRegex: Rule1[Expression] = rule("::r") {
    (quotedString | singleQuotedString) ~ "::r" ~~> { e => RegexExpression(e) }
  }
}
