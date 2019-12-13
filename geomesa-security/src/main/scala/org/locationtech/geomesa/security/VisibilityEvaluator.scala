/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.security

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.text.StringEscapeUtils
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
    Range.inclusive('a', 'z').foreach(chars(_) = true)
    Range.inclusive('A', 'Z').foreach(chars(_) = true)
    Range.inclusive('0', '9').foreach(chars(_) = true)
    Seq('_', '-', ':', '.', '/').foreach(chars(_) = true)
    chars
  }

  /**
    * Parses a visibility from a string. Results are cached for repeated calls
    *
    * Per standard operator precedence, the binary `&` has higher precedence than `|`, i.e.
    * `user|admin&test` is the same as `user|(admin&test)` and `user&admin|test` is the same as `(user&admin)|test`
    *
    * @param visibility visibility string, e.g. 'admin|user'
    * @param report provide detailed reporting on errors, or not
    * @throws org.parboiled.errors.ParsingException if visibility is not valid
    * @return parsed visibility expression
    */
  @throws(classOf[ParsingException])
  def parse(visibility: String, report: Boolean = false): VisibilityExpression = {
    if (visibility == null || visibility.isEmpty) { VisibilityNone } else {
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

    /**
      * Converts back to a string. The result can be converted back to a Visibility Expression again by
      * calling `VisbilityEvaluator.parse`
      *
      * @return the expression as a string
      */
    def expression: String
  }

  object VisibilityExpression {
    def apply(visibility: String): VisibilityExpression = parse(visibility)
    def unapply(visibility: VisibilityExpression): Option[String] = Some(visibility.expression)
  }

  /**
    * No visibility restrictions, can be seen by anyone
    */
  case object VisibilityNone extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = true
    override def expression: String = ""
  }

  /**
    * A specific visibility tag, which can only be seen by the equivalent authorization
    *
    * @param value visibility tag
    */
  case class VisibilityValue(value: Array[Byte]) extends VisibilityExpression {

    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean =
      authorizations.exists(java.util.Arrays.equals(value, _))

    override def expression: String = {
      val string = new String(value, StandardCharsets.UTF_8)
      if (value.forall(isValidAuthChar)) { string } else { '"' + StringEscapeUtils.escapeJava(string) + '"' }
    }

    override def equals(o: Any): Boolean = o match {
      case VisibilityValue(v) => java.util.Arrays.equals(value, v)
      case _ => false
    }

    override def hashCode(): Int = java.util.Arrays.hashCode(value)
  }

  /**
    * Boolean AND of visibilities
    *
    * @param expressions visibilities
    */
  case class VisibilityAnd(expressions: Seq[VisibilityExpression]) extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = expressions.forall(_.evaluate(authorizations))
    override def expression: String = {
      val clauses = expressions.map {
        case e: VisibilityOr => s"(${e.expression})"
        case e => e.expression
      }
      clauses.mkString("&")
    }
  }

  /**
    * Boolean OR of visibilities
    *
    * @param expressions visibilities
    */
  case class VisibilityOr(expressions: Seq[VisibilityExpression]) extends VisibilityExpression {
    override def evaluate(authorizations: Seq[Array[Byte]]): Boolean = expressions.exists(_.evaluate(authorizations))
    override def expression: String = {
      val clauses = expressions.map {
        case e: VisibilityAnd => s"(${e.expression})"
        case e => e.expression
      }
      clauses.mkString("|")
    }
  }

  private def isValidAuthChar(b: Byte): Boolean = validAuthChars(0xff & b)
}

class VisibilityEvaluator private extends BasicParser {

  import org.locationtech.geomesa.security.VisibilityEvaluator._
  import org.parboiled.scala._

  def visibility: Rule1[VisibilityExpression] = rule {
    expression ~ EOI
  }

  private def expression: Rule1[VisibilityExpression] = rule {
    oneOrMore(term, "|") ~~> (a => if (a.length == 1) { a.head } else { VisibilityOr(a) })
  }

  private def term: Rule1[VisibilityExpression] = rule {
    oneOrMore(factor, "&") ~~> (a => if (a.length == 1) { a.head } else { VisibilityAnd(a) })
  }

  private def factor: Rule1[VisibilityExpression] = rule { value | parens }

  private def parens: Rule1[VisibilityExpression] = rule { "(" ~ expression ~ ")" }

  private def value: Rule1[VisibilityExpression] = rule {
    string ~~> { s => VisibilityValue(s.getBytes(StandardCharsets.UTF_8))}
  }
}
