/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.common

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.locationtech.geomesa.index.iterators.AggregatingScan.RowValue
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType

/**
 * HBase mixin for aggregating scans
 *
 * @tparam T aggregate result type
 */
trait HBaseAggregator[T <: AggregatingScan.Result] extends AggregatingScan[T] {

  private val results = new java.util.ArrayList[Cell]
  private var scanner: RegionScanner = _
  private var more: Boolean = false
  private var iter: java.util.Iterator[Cell] = _
  private var lastscanned: Array[Byte] = _

  def setScanner(scanner: RegionScanner): Unit = {
    lastscanned = null
    this.scanner = scanner
    results.clear()
    more = scanner.next(results)
    iter = results.iterator()
  }

  def getLastScanned = lastscanned

  override protected def hasNextData: Boolean = iter.hasNext || more && {
    results.clear()
    more = scanner.next(results)
    iter = results.iterator()
    hasNextData
  }

  override protected def nextData(): RowValue = {
    val cell = iter.next()
    //println(s"Scanned ${ByteArrays.printable(cell.getRowArray)}")
    lastscanned = cell.getRowArray
    RowValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }
}

//class LimitingHBaseAggregator extends HBaseAggregator {
//  override protected def defaultBatchSize: Int = ???
//
//  /**
//    * Create the result object for the current scan
//    *
//    * @param sft       simple feature type
//    * @param transform transform, if any
//    * @param batchSize batch size
//    * @param options   scan options
//    * @return
//    */
//  override protected def createResult(sft: SimpleFeatureType, transform: Option[SimpleFeatureType], batchSize: Int, options: Map[String, String]): T = ???
//}
