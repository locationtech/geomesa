/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.schema

import com.google.gson.JsonObject
import org.apache.accumulo.access.{AccessExpression, IllegalAccessExpressionException}

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable

/** Complete file-wide AND/OR rollups of the expressions written to the visibility column.
  * Invalid expressions disable both proofs; they must never be dropped from a summary.
  * Exceeding a bound discards the entire summary; a partial summary must never be published.
  */
class VisibilityMetadata(fieldId: Int) {
  import VisibilityMetadata._

  private val expressions = mutable.LinkedHashSet.empty[String]
  private var rows = 0L
  private var bytes = 0
  private var valid = fieldId >= 0
  private var unrestricted = false

  def observe(expression: String): Unit = {
    rows += 1L
    if (expression == null || expression.isEmpty) unrestricted = true
    if (valid && expression != null && expression.nonEmpty && expressions.add(expression)) {
      // Avoid allocating UTF-8 bytes when the character count already exceeds the limit.
      bytes += (if (expression.length > MaxBytes) MaxBytes + 1 else expression.getBytes(UTF_8).length)
      if (expressions.size > MaxExpressions || bytes > MaxBytes) {
        discard()
      }
    }
  }

  def metadata: Option[String] = {
    if (!valid) { None } else {
      try {
        // Validate and normalize only at file close, after exact-string deduplication. Every
        // operand is parenthesized so compound AND/OR expressions retain their meaning.
        val operands = expressions.iterator.map(expression => s"($expression)").toSeq
        val combined = operands.mkString("&")
        if (!withinDepthLimit(combined)) return None
        val allOf = AccessExpression.of(combined, true).getExpression
        // Validate all operands above even when an unrestricted row makes the OR rollup true.
        val anyOf = if (unrestricted) "" else AccessExpression.of(operands.mkString("|"), true).getExpression
        val json = new JsonObject()
        json.addProperty("version", 1)
        json.addProperty("fieldId", fieldId)
        json.addProperty("scope", "file")
        json.addProperty("rowCount", rows)
        json.addProperty("allOf", allOf)
        json.addProperty("anyOf", anyOf)
        val encoded = json.toString
        if (encoded.getBytes(UTF_8).length > MaxBytes) None else Some(encoded)
      } catch {
        case _: IllegalAccessExpressionException => None
        case _: StackOverflowError => None // optional metadata must not fail file finalization
      }
    }
  }

  private def discard(): Unit = {
    valid = false
    expressions.clear()
  }
}

object VisibilityMetadata {
  val MetadataKey = "geomesa.visibility.rollup"
  // Conservative application limits, not Parquet format limits: bound the number of operands
  // and the memory used for optional visibility summaries. MaxBytes caps both accumulated
  // UTF-8 expression bytes and the final JSON footer value, including rollups and escaping.
  val MaxExpressions = 1024
  val MaxBytes = 64 * 1024
  val MaxDepth = 128

  /** Bound recursive parsing without counting parentheses inside quoted authorization labels. */
  def withinDepthLimit(expression: String): Boolean = {
    var depth = 0
    var quoted = false
    var escaped = false
    var i = 0
    while (i < expression.length) {
      val c = expression.charAt(i)
      if (quoted) {
        if (escaped) escaped = false
        else if (c == '\\') escaped = true
        else if (c == '"') quoted = false
      } else if (c == '"') {
        quoted = true
      } else if (c == '(') {
        depth += 1
        if (depth > MaxDepth) return false
      } else if (c == ')') {
        depth -= 1
        if (depth < 0) return false
      }
      i += 1
    }
    depth == 0 && !quoted
  }
}
