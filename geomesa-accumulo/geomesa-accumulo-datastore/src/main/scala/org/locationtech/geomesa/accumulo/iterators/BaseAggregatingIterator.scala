/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data.{Range => aRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.accumulo.iterators.BaseAggregatingIterator.BatchScanCallback
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.locationtech.geomesa.index.iterators.AggregatingScan.{AggregateCallback, RowValue}

/**
 * Aggregating iterator - only works on kryo-encoded features
 */
abstract class BaseAggregatingIterator[T <: AggregatingScan.Result]
    extends SortedKeyValueIterator[Key, Value] with AggregatingScan[T] {

  import scala.collection.JavaConverters._

  var source: SortedKeyValueIterator[Key, Value] = _

  protected var topKey: Key = _
  private var topValue: Value = new Value()
  private var currentRange: aRange = _
  private var needToAdvance = false

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src
    super.init(options.asScala.toMap)
  }

  override def hasTop: Boolean = topKey != null
  override def getTopKey: Key = topKey
  override def getTopValue: Value = topValue

  override def seek(range: aRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    currentRange = range
    source.seek(range, columnFamilies, inclusive)
    needToAdvance = false
    findTop()
  }

  override def next(): Unit = findTop()

  private def findTop(): Unit = {
    val result = aggregate(new BatchScanCallback()).result
    if (result == null) {
      topKey = null // hasTop will be false
      topValue = null
    } else {
      if (topValue == null) {
        // only re-create topValue if it was nulled out
        topValue = new Value()
      }
      topValue.set(result)
    }
  }

  override protected def hasNextData: Boolean = {
    if (needToAdvance) {
      source.next() // advance the source iterator, this may invalidate the top key/value we've already read
      needToAdvance = false
    }
    source.hasTop && !currentRange.afterEndKey(source.getTopKey)
  }

  override protected def nextData(): RowValue = {
    needToAdvance = true
    topKey = source.getTopKey
    val value = source.getTopValue.get()
    RowValue(topKey.getRow.getBytes, 0, topKey.getRow.getLength, value, 0, value.length)
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError()
}

object BaseAggregatingIterator {

  private class BatchScanCallback extends AggregateCallback {

    private var bytes: Array[Byte] = _

    override def batch(bytes: Array[Byte]): Boolean = {
      this.bytes = bytes
      false // we want to stop scanning and return the batch
    }

    // we always keep scanning and rely on client connections to stop the scan
    override def partial(bytes: => Array[Byte]): Boolean = true

    def result: Array[Byte] = bytes
  }
}
