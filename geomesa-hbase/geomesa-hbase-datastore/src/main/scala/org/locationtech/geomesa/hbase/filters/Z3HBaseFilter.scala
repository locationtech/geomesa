/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{Filter, FilterBase}
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.utils.cache.ByteArrayCacheKey
import org.locationtech.geomesa.utils.index.ByteArrays

class Z3HBaseFilter(filter: Z3Filter, offset: Int, bytes: Array[Byte]) extends FilterBase {

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    if (filter.inBounds(v.getRowArray, v.getRowOffset + offset)) {
      Filter.ReturnCode.INCLUDE
    } else {
      Filter.ReturnCode.SKIP
    }
  }

  override def toByteArray: Array[Byte] = bytes

  override def toString: String = s"Z3HBaseFilter[$filter]"
}

object Z3HBaseFilter extends StrictLogging {

  import org.locationtech.geomesa.index.ZFilterCacheSize

  val Priority = 20

  private val cache = Caffeine.newBuilder().maximumSize(ZFilterCacheSize.toInt.get).build(
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
