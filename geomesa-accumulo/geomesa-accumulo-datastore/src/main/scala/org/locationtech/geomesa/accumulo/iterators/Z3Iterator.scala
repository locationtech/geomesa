/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.geomesa.index.index.z3.Z3IndexValues

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator._

  private var source: SortedKeyValueIterator[Key, Value] = _

  private var filter: Z3Filter = _
  private var zOffset: Int = -1

  private var topKey: Key = _
  private var topValue: Value = _
  private val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    import scala.collection.JavaConverters._

    this.source = source

    zOffset = options.get(Config.ZOffsetKey).toInt
    filter = Z3Filter.deserializeFromStrings(options.asScala)
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
      if (filter.inBounds(row.getBytes, zOffset)) {
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
    import scala.collection.JavaConversions._
    val iter = new Z3Iterator
    val opts = Z3Filter.serializeToStrings(filter) + (Config.ZOffsetKey -> zOffset.toString)
    iter.init(source.deepCopy(env), opts, env)
    iter
  }
}

object Z3Iterator {

  object Config {
    val ZOffsetKey = "zo"
  }

  def configure(values: Z3IndexValues,
                hasSplits: Boolean,
                isSharing: Boolean,
                priority: Int): IteratorSetting = {

    val offset = if (isSharing && hasSplits) { "2" } else if (isSharing || hasSplits) { "1" } else { "0" }

    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])
    is.addOption(Config.ZOffsetKey, offset)
    // index space values for comparing in the iterator
    Z3Filter.serializeToStrings(Z3Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    is
  }
}