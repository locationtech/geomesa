/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schema

import java.util.Locale

/**
 * Mapping of a feature-type attribute to a valid column name (lowercased, only alphanumeric/underscore)
 *
 * @param attribute feature type attribute
 * @param column storage column name
 */
case class ColumnName(attribute: String, column: String)

/**
 * Provides mappings to and from attribute names and valid parquet column names
 */
object ColumnName {

  /**
   * Encode an external name into a valid storage column name
   *
   * @param name external user-provided name
   * @return
   */
  def encode(name: String): String = {
    if (name.startsWith("__")) {
      // internal field (fid, bbox, etc)
      name
    } else {
      name.toLowerCase(Locale.US).replaceAll("[^a-z0-9_]", "_").replaceAll("__+", "_")
    }
  }
  /**
   * Convert an attribute name into a valid parquet column name
   *
   * @param name attribute name
   * @return
   */
  def apply(name: String): ColumnName = ColumnName(name, encode(name))
}
