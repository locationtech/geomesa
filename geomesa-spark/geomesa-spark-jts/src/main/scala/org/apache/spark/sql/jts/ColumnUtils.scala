/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.apache.spark.sql.jts

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ExpressionUtils
import org.apache.spark.sql.internal.{Alias, ColumnNode, UnresolvedAttribute, UnresolvedFunction}

/**
 * Bridges between Spark's public `Column` type and Catalyst internals. As of Spark 4 `Column` is
 * backed by a `ColumnNode` rather than an `Expression`, and the relevant conversion helpers and node
 * types are `private[sql]`. This object lives in an `org.apache.spark` package so it can access them,
 * exposing the pieces the rest of the module needs.
 */
object ColumnUtils {

  /** Create an `Expression`-backed `Column`. */
  def toColumn(expression: Expression): Column = ExpressionUtils.column(expression)

  /**
   * Derives a human-readable name for a column, used to build the aliases assigned to the results of
   * the JTS UDFs (e.g. `st_contains(a,b)`). Operates on the underlying `ColumnNode` tree since, as of
   * Spark 4, that is what a `Column` wraps.
   */
  def columnName(column: Column): String = {
    column.node match {
      case ua: UnresolvedAttribute => ua.nameParts.mkString(".")
      case al: Alias => al.name.mkString(".")
      case uf: UnresolvedFunction => uf.functionName
      case node: ColumnNode => node.sql
    }
  }
}
