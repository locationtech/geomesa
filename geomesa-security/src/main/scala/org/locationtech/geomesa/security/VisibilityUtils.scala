/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.security

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.access.ParsedAccessExpression.ExpressionType
import org.apache.accumulo.access.{Access, ParsedAccessExpression}
import org.geotools.api.feature.simple.SimpleFeature

import scala.util.control.NonFatal

object VisibilityUtils {

  private val access = Access.builder().build()

  /**
   * Return a local function that will check features for visibility, based on the user's current authorizations.
   * Not thread-safe or re-usable in a subsequent request.
   *
   * @param provider auth provider
   * @return
   */
  def visible(provider: AuthorizationsProvider): SimpleFeature => Boolean =
    new AuthVisibilityFeatureCheck(new java.util.HashSet(provider.getAuthorizations))

  /**
   * Return a local function that will check visibility strings directly, based on the user's current authorizations.
   * Not thread-safe or re-usable in a subsequent request.
   *
   * @param provider auth provider
   * @return
   */
  def check(provider: AuthorizationsProvider): String => Boolean =
    new AuthVisibilityCheck(new java.util.HashSet[String](provider.getAuthorizations))

  /**
   * Normalize and simplify a visibility expression
   *
   * @param expression expression
   * @return
   */
  @throws[IllegalArgumentException]
  def normalize(expression: String): String = normalize(access.newParsedExpression(expression))

  /**
   * Normalize and simplify a visibility expression
   *
   * @param expression expression
   * @return
   */
  def normalize(expression: ParsedAccessExpression): String = NormalizedExpression.normalize(expression).expression

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityFeatureCheck(auths: java.util.Set[String]) extends (SimpleFeature => Boolean) {

    private val check = new AuthVisibilityCheck(auths)

    /**
     * Checks auths against the feature's visibility
     *
     * @param f feature
     * @return true if feature is visible to the current user, otherwise false
     */
    override def apply(f: SimpleFeature): Boolean = check(SecurityUtils.getVisibility(f))
  }

  /**
   * Parses any visibilities in the feature and compares with the user's authorizations
   *
   * @param auths authorizations for the current user
   */
  private class AuthVisibilityCheck(auths: java.util.Set[String]) extends (String => Boolean) with LazyLogging {

    private val access = VisibilityUtils.access.newEvaluator(auths)
    private val cache = scala.collection.mutable.Map.empty[String, Boolean]

    /**
     * Checks auths against visibility
     *
     * @param vis visibility string
     * @return true if feature is visible to the current user, otherwise false
     */
    override def apply(vis: String): Boolean = vis == null || cache.getOrElseUpdate(vis, canAccess(vis))

    private def canAccess(vis: String): Boolean = {
      try { access.canAccess(vis) } catch {
        case NonFatal(e) =>
          logger.warn("Error evaluating visibility expression:", e)
          false
      }
    }
  }

  // the following NormalizedExpression code is taken from accumulo-access examples under the Apache 2 license:

  private object NormalizedExpression  {

    /**
     * <p>
     * For a given access expression this example will deduplicate, sort, flatten, and remove unneeded
     * parentheses or quotes in the expressions. The following list gives examples of what each
     * normalization step does.
     *
     * <ul>
     * <li>As an example of flattening, the expression {@code A&(B&C)} flattens to {@code
     * A&B&C}.</li>
     * <li>As an example of sorting, the expression {@code (Z&Y)|(C&B)} sorts to {@code
     * (B&C)|(Y&Z)}</li>
     * <li>As an example of deduplication, the expression {@code X&Y&X} normalizes to {@code X&Y}</li>
     * <li>As an example of unneeded quotes, the expression {@code "ABC"&"XYZ"} normalizes to
     * {@code ABC&XYZ}</li>
     * <li>As an example of unneeded parentheses, the expression {@code (((ABC)|(XYZ)))} normalizes to
     * {@code ABC|XYZ}</li>
     * </ul>
     *
     * <p>
     * This algorithm attempts to have the same behavior as the one in the Accumulo 2.1
     * ColumnVisibility class. However the implementation is very different.
     */
    def normalize(parsed: ParsedAccessExpression): NormalizedExpression = {
      if (parsed.getType == ExpressionType.AUTHORIZATION) {
        // If the authorization is quoted and it does not need to be quoted then the following two
        // lines will remove the unnecessary quoting.
        val unquoted = access.unquote(parsed.getExpression)
        val quoted = access.quote(unquoted)
        new NormalizedExpression(quoted, parsed.getType)
      } else {
        // The tree set does the work of sorting and deduplicating sub expressions.
        val normalizedChildren = new java.util.TreeSet[NormalizedExpression]()
        parsed.getChildren.forEach(child => flatten(parsed.getType, child, normalizedChildren))

        if (normalizedChildren.size() == 1) {
          normalizedChildren.first()
        } else {
          val operator = if (parsed.getType == ExpressionType.AND) { "&" } else { "|" }
          var sep = ""

          val builder = new StringBuilder()

          normalizedChildren.forEach { child =>
            builder.append(sep)
            if (child.expressionType == ExpressionType.AUTHORIZATION) {
              builder.append(child.expression)
            } else {
              builder.append("(")
              builder.append(child.expression)
              builder.append(")")
            }
            sep = operator
          }
          new NormalizedExpression(builder.toString(), parsed.getType)
        }
      }
    }

    /**
     * This method helps with the flattening aspect of normalization by recursing down as far as
     * possible the parse tree in the case when the expression type is the same. As long as the type
     * is the same in the sub expression, keep using the same set.
     */
    private def flatten(
      parentType: ExpressionType,
      parsed: ParsedAccessExpression,
      normalizedExpressions: java.util.SortedSet[NormalizedExpression]): Unit = {
      if (parsed.getType == parentType) {
        parsed.getChildren.forEach(child => flatten(parentType, child, normalizedExpressions))
      } else {
        // The type changed, so start again on the subexpression.
        normalizedExpressions.add(normalize(parsed))
      }
    }

    // determines the sort order of different kinds of subexpressions.
    private def typeOrder(expressionType: ExpressionType): Int = expressionType match {
      case ExpressionType.AUTHORIZATION => 1
      case ExpressionType.OR            => 2
      case ExpressionType.AND           => 3
      case ExpressionType.EMPTY => throw new IllegalArgumentException(s"Unexpected type: $expressionType")
    }
  }
  /**
   * As part of normalizing access expression this class is used to sort and dedupe sub-expressions
   * in a tree set.
   */
  private case class NormalizedExpression(expression: String, expressionType: ExpressionType)
      extends Comparable[NormalizedExpression] {

    override def compareTo(o: NormalizedExpression): Int = {
      var cmp = NormalizedExpression.typeOrder(expressionType) - NormalizedExpression.typeOrder(o.expressionType)
      if (cmp == 0) {
        if (expressionType == ExpressionType.AUTHORIZATION) {
          // sort based on the unquoted and unescaped form of the authorization
          cmp = access.unquote(expression).compareTo(access.unquote(o.expression))
        } else {
          cmp = expression.compareTo(o.expression)
        }
      }
      cmp
    }

    override def equals(obj: Any): Boolean =
      this == obj || (canEqual(obj) && obj.asInstanceOf[NormalizedExpression].compareTo(this) == 0)

    override def hashCode(): Int = {
      if (expressionType == ExpressionType.AUTHORIZATION) {
        access.unquote(expression).hashCode
      } else {
        expression.hashCode
      }
    }
  }
}
