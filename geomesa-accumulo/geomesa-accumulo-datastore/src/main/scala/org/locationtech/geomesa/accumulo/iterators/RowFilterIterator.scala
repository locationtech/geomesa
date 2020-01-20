/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.index.filters.RowFilter
import org.locationtech.geomesa.index.filters.RowFilter.RowFilterFactory

/**
 * Abstract base class for filtering iterators. Implementations must have a no-arg default constructor, used
 * when deep copying an instance.
 *
 * @param factory filter factory
 * @tparam T filter type bound
 */
abstract class RowFilterIterator[T <: RowFilter](factory: RowFilterFactory[T])
    extends SortedKeyValueIterator[Key, Value] {

  import scala.collection.JavaConverters._

  private var source: SortedKeyValueIterator[Key, Value] = _

  private var filter: T = _
  private var offset: Int = -1

  private var topKey: Key = _
  private var topValue: Value = _
  private val row = new Text()

  override def init(
      source: SortedKeyValueIterator[Key, Value],
      options: java.util.Map[String, String],
      env: IteratorEnvironment): Unit = {
    this.source = source
    offset = options.get(RowFilterIterator.RowOffsetKey).toInt
    filter = factory.deserializeFromStrings(options.asScala)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  private def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop) {
      source.getTopKey.getRow(row)
      if (filter.inBounds(row.getBytes, offset)) {
        topKey = source.getTopKey
        topValue = source.getTopValue
        return
      } else {
        source.next()
      }
    }
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val opts = factory.serializeToStrings(filter) + (RowFilterIterator.RowOffsetKey -> offset.toString)
    val iter = getClass.newInstance() // note: requires default (zero-arg) constructor
    iter.init(source.deepCopy(env), opts.asJava, env)
    iter
  }
}

object RowFilterIterator {
  val RowOffsetKey = "zo"
}
