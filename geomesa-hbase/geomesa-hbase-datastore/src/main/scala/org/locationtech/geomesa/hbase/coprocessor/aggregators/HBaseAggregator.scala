/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.locationtech.geomesa.index.iterators.AggregatingScan

trait HBaseAggregator[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }] extends AggregatingScan[T] {

  private val results = new java.util.ArrayList[Cell]
  private var scanner: RegionScanner = _
  private var more: Boolean = false
  private var iter: java.util.Iterator[Cell] = _

  def setScanner(scanner: RegionScanner): Unit = {
    this.scanner = scanner
    results.clear()
    more = scanner.next(results)
    iter = results.iterator()
  }

  override def hasNextData: Boolean = iter.hasNext || more && {
    results.clear()
    more = scanner.next(results)
    iter = results.iterator()
    hasNextData
  }

  override def nextData(setValues: (Array[Byte], Int, Int, Array[Byte], Int, Int) => Unit): Unit = {
    val cell = iter.next()
    setValues(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }
}
