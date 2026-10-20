/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package functions

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.accumulo.access.ParsedAccessExpression.ExpressionType
import org.apache.accumulo.access.{Access, ParsedAccessExpression}
import org.locationtech.geomesa.gt.partition.postgis.dialect.SqlStatements

import java.util.concurrent.TimeUnit

/**
 * Provides per-row visibility filtering
 */
object PgVis extends PgVis with AdvisoryLockSql {

  import scala.collection.JavaConverters._

  private val access = Access.builder().build()

  private val toPostgisCache =
    Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(key: String): String = toPostgis(access.newParsedExpression(key))
      }
    )

  private val fromPostgisCache =
    Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(key: String): String = fromPostgis(key)
      }
    )

  override protected val lockId: Long = 1957550962396498252L

  /**
   * Converts an Accumulo expressions to a tsvector expression, suitable for visibility evaluation in postgis
   *
   * @param vis visibility expression
   * @return
   */
  def toPostgisVisibility(vis: String): String = toPostgisCache.get(vis)

  /**
   * Converts a tsvector expressions back to an Accumulo expression
   *
   * @param vis visibility expression
   * @return
   */
  def fromPostgisVisibility(vis: String): String = fromPostgisCache.get(vis)

  /**
   * Converts an Accumulo expressions to a tsvector expression. Expressions are wrapped in single quotes, and single quotes
   * and back-slashes within a term are doubled to escape them.
   *
   * @param expression expression
   * @return
   */
  private def toPostgis(expression: ParsedAccessExpression): String = {
    expression.getType match {
      case ExpressionType.AUTHORIZATION => s"'${access.unquote(expression.getExpression).replaceAll("(['\\\\])", "$0$0")}'"
      case ExpressionType.AND => expression.getChildren.asScala.map(toPostgis).filter(_.nonEmpty).mkString("(", " & ", ")")
      case ExpressionType.OR => expression.getChildren.asScala.map(toPostgis).filter(_.nonEmpty).mkString("(", " | ", ")")
      case ExpressionType.EMPTY => ""
    }
  }

  /**
   * Converts a tsvector expression back to an Accumulo expression.
   *
   * @param expression expression
   * @return
   */
  private def fromPostgis(expression: String): String = {
    val result = new StringBuilder(expression.length)
    var index = 0
    while (index < expression.length) {
      expression.charAt(index) match {
        case c if Character.isWhitespace(c) =>
          index += 1

        case '(' | ')' | '&' | '|' =>
          result.append(expression.charAt(index))
          index += 1

        case '\'' =>
          val authorization = new StringBuilder()
          index += 1
          var closed = false
          while (index < expression.length && !closed) {
            expression.charAt(index) match {
              case '\'' if index + 1 < expression.length && expression.charAt(index + 1) == '\'' =>
                authorization.append('\'')
                index += 2
              case '\'' =>
                closed = true
                index += 1
              case '\\' if index + 1 < expression.length && expression.charAt(index + 1) == '\\' =>
                authorization.append('\\')
                index += 2
              case '\\' =>
                throw new IllegalArgumentException(s"Invalid escape sequence at index $index: $expression")
              case character =>
                authorization.append(character)
                index += 1
            }
          }
          if (!closed) {
            throw new IllegalArgumentException(s"Unterminated authorization at index $index: $expression")
          }
          result.append(access.quote(authorization.toString))

        case _ =>
          throw new IllegalArgumentException(s"Unexpected character at index $index: $expression")
      }
    }
    result.toString
  }
}

class PgVis extends SqlStatements {

  override protected def createStatements(info: TypeInfo): Seq[String] = {
    Seq(
      """-- Evaluate visibilities against authorizations.
        |--
        |-- Note that visibility strings are expected to be well formed - invalid strings will not raise errors but will
        |-- always evaluate to 'false'. Empty/null strings are considered invalid.
        |--
        |-- A valid expression is a sequence of tokens (chars from [A-Za-z0-9_-.:/], or any chars if quoted with ' or ")
        |-- joined by the binary operators & (and) or | (or). & and | may not be mixed at the same level without
        |-- parentheses to disambiguate, e.g. 'A|B&C' is invalid but '(A|B)&C' and 'A|(B&C)' are valid.
        |--
        |-- arguments:
        |--   vis         - visibility expression, consisting of tokens separated by & and |
        |--   auth_vector - user authorization tokens
        |-- returns: the result of evaluating the visibilities against the authorizations, i.e. true if the user can see the piece of data
        |CREATE OR REPLACE FUNCTION pg_vis(vis text, auth_vector tsvector) RETURNS boolean AS $BODY$
        |  BEGIN
        |    IF vis IS NULL OR vis = '' THEN
        |      return false;
        |    END IF;
        |    -- perform the fast native C-level evaluation
        |    RETURN auth_vector @@ vis::tsquery;
        |  EXCEPTION
        |    -- invalid syntax errors exclude the row
        |    WHEN OTHERS THEN
        |      RAISE WARNING 'Invalid visibility expression: %', vis;
        |      RETURN false;
        |  END;
        |$BODY$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
        |""".stripMargin
    )
  }

  override protected def dropStatements(info: TypeInfo): Seq[String] = Seq.empty // function is shared between types
}
