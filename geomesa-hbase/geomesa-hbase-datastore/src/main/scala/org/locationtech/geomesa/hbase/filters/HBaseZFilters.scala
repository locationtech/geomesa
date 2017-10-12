/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters

import com.google.common.primitives.{Bytes, Ints}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{Filter, FilterBase}
import org.locationtech.geomesa.index.filters.{Z2Filter, Z3Filter}

class Z3HBaseFilter(val filter: Z3Filter, offset: Int) extends FilterBase with LazyLogging {

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    logger.trace("In filterKeyValue()")
    if (filter.inBounds(v.getRowArray, v.getRowOffset + offset)) {
      Filter.ReturnCode.INCLUDE
    } else {
      Filter.ReturnCode.SKIP
    }
  }

  override def toByteArray: Array[Byte] = {
    logger.trace("Serializing Z3HBaseFilter")
    Bytes.concat(Z3Filter.serializeToBytes(filter), Ints.toByteArray(offset))
  }

  override def toString: String = s"Z3HBaseFilter[$filter]"
}

object Z3HBaseFilter extends LazyLogging {
  val Priority = 20

  @throws[DeserializationException]
  def parseFrom(pbBytes: Array[Byte]): Filter = {
    logger.trace("Deserializing Z3HBaseFilter")
    val offset = Ints.fromBytes(pbBytes(pbBytes.length - 4), pbBytes(pbBytes.length - 3),
      pbBytes(pbBytes.length - 2), pbBytes(pbBytes.length - 1))
    new Z3HBaseFilter(Z3Filter.deserializeFromBytes(pbBytes), offset)
  }
}

class Z2HBaseFilter(val filter: Z2Filter, offset: Int) extends FilterBase with LazyLogging {

  override def filterKeyValue(v: Cell): Filter.ReturnCode = {
    logger.trace("In filterKeyValue()")
    if (filter.inBounds(v.getRowArray, v.getRowOffset + offset)) {
      Filter.ReturnCode.INCLUDE
    } else {
      Filter.ReturnCode.SKIP
    }
  }

  override def toByteArray: Array[Byte] = {
    logger.trace("Serializing Z2HBaseFilter")
    Bytes.concat(Z2Filter.serializeToBytes(filter), Ints.toByteArray(offset))
  }

  override def toString: String = s"Z2HBaseFilter[$filter]"
}

object Z2HBaseFilter extends LazyLogging {
  val Priority = 20

  @throws[DeserializationException]
  def parseFrom(pbBytes: Array[Byte]): Filter = {
    logger.trace("Deserializing Z2HBaseFilter")
    val offset = Ints.fromBytes(pbBytes(pbBytes.length - 4), pbBytes(pbBytes.length - 3),
      pbBytes(pbBytes.length - 2), pbBytes(pbBytes.length - 1))
    new Z2HBaseFilter(Z2Filter.deserializeFromBytes(pbBytes), offset)
  }
}
