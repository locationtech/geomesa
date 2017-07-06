/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import org.locationtech.geomesa.utils.text.BasicParser
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala.parserunners.{BasicParseRunner, ReportingParseRunner}

/**
  * Evaluates visibilities against authorizations. Abstracted from Accumulo visibility code
  */
object VisibilityEvaluator {

  private val Parser = new VisibilityEvaluator()

  private val cache = new ConcurrentHashMap[String, VisibilityExpression]

  // copied from org.apache.accumulo.core.security.Authorizations
  private val validAuthChars = {
    val chars = Array.fill[Boolean](256)(false)
    Range('a', 'z').foreach(chars(_) = true)
    Range('A', 'Z').foreach(chars(_) = true)
    Range('0', '9').foreach(chars(_) = true)
    Seq('_', '-', ':', '.', '/').foreach(chars(_) = true)
    chars
  }

  /**
    * Parses a visibility from a string. Results are cached for repeated calls
    *
    * @param visibility visibility string, e.g. 'admin|user'
    * @param report provide detailed reporting on errors, or not
    * @throws org.parboiled.errors.ParsingException if visibility is not valid
    * @return parsed visibility expression
    */
  @throws(classOf[ParsingException])
  def parse(visibility: String, report: Boolean = false): VisibilityExpression = {
    if (visibility == null || visibility.isEmpty) {
      VisibilityNone
    } else {
      var parsed = cache.get(visibility)
      if (parsed == null) {
        val runner = if (report) { ReportingParseRunner(Parser.visibility) } else { BasicParseRunner(Parser.visibility) }
        val parsing = runner.run(visibility)
        parsed = parsing.result.getOrElse(throw new ParsingException(ErrorUtils.printParseErrors(parsing)))
        cache.put(visibility, parsed)
      }
      parsed
    }
  }

  /**
    * Parsed visibility that can be evaluated
    */
  sealed trait VisibilityExpression {

    /**
      * Checks if the data tagged with this visibility can be seen or not
      *
      * @param authorizations authorizations of the user attempting to access the tagged data
      * @return true if can see, otherwise false
      */
    def evaluate(authorizations: Seq[Array[Byte]]): Boolean
  }

  /**
    * No visibility restrictions, can be seen by anyone
    */
  case object VisibilityNone extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = true
  }

  /**
    * A specific visibility tag, which can only be seen by the equivalent authorization
    *
    * @param value visibility tag
    */
  case class VisibilityValue(value: Array[Byte]) extends VisibilityExpression {

    require(value.forall(isValidAuthChar), s"Invalid character in '${new String(value, StandardCharsets.UTF_8)}'")

    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean =
      authorizations.exists(java.util.Arrays.equals(value, _))

    override def equals(o: Any): Boolean = {
      o match {
        case VisibilityValue(v) => java.util.Arrays.equals(value, v)
        case _ => false
      }
    }
  }

  /**
    * Boolean AND of visibilities
    *
    * @param expressions visibilities
    */
  case class VisibilityAnd(expressions: Seq[VisibilityExpression]) extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = expressions.forall(_.evaluate(authorizations))
  }

  /**
    * Boolean OR of visibilities
    *
    * @param expressions visibilities
    */
  case class VisibilityOr(expressions: Seq[VisibilityExpression]) extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = expressions.exists(_.evaluate(authorizations))
  }

  private def isValidAuthChar(b: Byte): Boolean = validAuthChars(0xff & b)
}

class VisibilityEvaluator private extends BasicParser {

  import org.locationtech.geomesa.security.VisibilityEvaluator.{VisibilityAnd, VisibilityExpression, VisibilityOr, VisibilityValue}
  import org.parboiled.scala._

  def visibility: Rule1[VisibilityExpression] = rule {
    expression ~ EOI
  }

  private def expression: Rule1[VisibilityExpression] = rule {
    oneOrMore(term, "&") ~~> ((a) => if (a.length == 1) { a.head } else { VisibilityAnd(a) })
  }

  private def term: Rule1[VisibilityExpression] = rule {
    oneOrMore(factor, "|") ~~> ((a) => if (a.length == 1) { a.head } else { VisibilityOr(a) })
  }

  private def factor: Rule1[VisibilityExpression] = rule { value | parens }

  private def parens = rule { "(" ~ expression ~ ")" }

  private def value: Rule1[VisibilityExpression] = rule {
    string ~~> { s => VisibilityValue(s.getBytes(StandardCharsets.UTF_8))}
  }
}
