/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

trait RowFilter {
  def inBounds(buf: Array[Byte], offset: Int): Boolean
}

object RowFilter {

  trait RowFilterFactory[T <: RowFilter] {
    def serializeToBytes(filter: T): Array[Byte]
    def deserializeFromBytes(serialized: Array[Byte]): T

    def serializeToStrings(filter: T): Map[String, String]
    def deserializeFromStrings(serialized: scala.collection.Map[String, String]): T
  }
}
