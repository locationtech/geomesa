/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.filter

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{Filter, FilterBase}
import org.apache.hadoop.hbase.{Cell, KeyValue}
import org.locationtech.geomesa.index.filters.RowFilter.FilterResult
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.utils.cache.ByteArrayCacheKey
import org.locationtech.geomesa.utils.index.ByteArrays

class Z3HBaseFilter(filter: Z3Filter, offset: Int, bytes: Array[Byte]) extends FilterBase {

  private var nextBytes: Array[Byte] = _

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    filter.filter(v.getRowArray, v.getRowOffset + offset) match {
      case FilterResult.InBounds => Filter.ReturnCode.INCLUDE
      case FilterResult.OutOfBounds => Filter.ReturnCode.SKIP
      case FilterResult.SkipAhead(next) => nextBytes = next; Filter.ReturnCode.SEEK_NEXT_USING_HINT
    }
  }

  override def getNextCellHint(v: Cell): Cell = {
    val row = Array.ofDim[Byte](nextBytes.length + offset)
    System.arraycopy(v.getRowArray, v.getRowOffset, row, 0, offset)
    System.arraycopy(nextBytes, 0, row, offset, nextBytes.length)
    new KeyValue(
      row,
      0,
      row.length,
      v.getFamilyArray,
      v.getFamilyOffset,
      v.getFamilyLength,
      v.getQualifierArray,
      v.getQualifierOffset,
      v.getQualifierLength,
      v.getTimestamp,
      KeyValue.Type.Maximum,
      Array.empty,
      0,
      0
    )
  }

  override def toByteArray: Array[Byte] = bytes

  override def toString: String = s"Z3HBaseFilter[$filter]"
}

object Z3HBaseFilter extends StrictLogging {

  import org.locationtech.geomesa.index.ZFilterCacheSize

  val Priority = 20

  private val cache: LoadingCache[ByteArrayCacheKey, (Z3Filter, Int)] =
    Caffeine.newBuilder().maximumSize(ZFilterCacheSize.toInt.get).build(
      new CacheLoader[ByteArrayCacheKey, (Z3Filter, Int)]() {
        override def load(key: ByteArrayCacheKey): (Z3Filter, Int) = {
          val filter = Z3Filter.deserializeFromBytes(key.bytes)
          val offset = ByteArrays.readInt(key.bytes, key.bytes.length - 4)
          logger.trace(s"Deserialized $offset:$filter")
          (filter, offset)
        }
      }
    )

  /**
    * Create a new filter. Typically, filters created by this method will just be serialized to bytes and sent
    * as part of an HBase scan
    *
    * @param filter z3 filter
    * @param offset row offset
    * @return
    */
  def apply(filter: Z3Filter, offset: Int): Z3HBaseFilter = {
    val filterBytes = Z3Filter.serializeToBytes(filter)
    val serialized = Array.ofDim[Byte](filterBytes.length + 4)
    System.arraycopy(filterBytes, 0, serialized, 0, filterBytes.length)
    ByteArrays.writeInt(offset, serialized, filterBytes.length)
    new Z3HBaseFilter(filter, offset, serialized)
  }

  /**
    * Override of static method from org.apache.hadoop.hbase.filter.Filter
    *
    * @param pbBytes serialized bytes
    * @throws org.apache.hadoop.hbase.exceptions.DeserializationException serialization exception
    * @return
    */
  @throws[DeserializationException]
  def parseFrom(pbBytes: Array[Byte]): Filter = {
    val (filter, offset) = cache.get(new ByteArrayCacheKey(pbBytes))
    new Z3HBaseFilter(filter, offset, pbBytes)
  }
}
