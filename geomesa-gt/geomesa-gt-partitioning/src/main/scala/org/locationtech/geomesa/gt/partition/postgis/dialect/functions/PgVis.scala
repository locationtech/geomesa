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

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * Provides per-row visibility filtering
 */
object PgVis {

  import scala.collection.JavaConverters._

  private val access = Access.builder().build()

  private val toPostgisCache =
    Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(key: String): String = toPostgis(key)
      }
    )

  private val fromPostgisCache =
    Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(key: String): String = fromPostgis(key)
      }
    )

  private val PostgisVisPattern = Pattern.compile("""^\(*'""")

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
   * Converts an Accumulo expressions to a tsvector expression
   *
   * @param expression visibility expression
   * @return
   */
  private def toPostgis(expression: String): String = {
    // parse first to ensure a valid expression
    val parsed = access.newParsedExpression(expression)
    if (parsed.getType == ExpressionType.EMPTY) {
      throw new IllegalArgumentException("Invalid empty visibility label")
    }
    // but if possible, just use the original expression
    if (!needsEscaping(expression)) {
      expression
    } else {
      // we have to use tsvector quoting/escaping instead of accumulo
      toPostgis(parsed)
    }
  }

  private def needsEscaping(expression: String): Boolean = {
    var i = 0
    while (i < expression.length) {
      val c = expression.charAt(i)
      if ((c >= 'A' && c <= 'Z') ||
          (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') ||
          c == '_'|| c == '(' || c == ')' || c == '&' || c == '|') {
        i += 1
      } else {
        return true
      }
    }
    false
  }

  /**
   * Converts an Accumulo expressions to a tsvector expression. Expressions are wrapped in single quotes, and single quotes
   * and back-slashes within a term are doubled to escape them.
   *
   * @param expression visibility expression
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
    if (!PostgisVisPattern.matcher(expression).find()) {
      // assumed to be an original, non-escaped expression
      return expression
    }
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
