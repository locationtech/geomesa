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
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.geomesa.index.index.z2.Z2IndexValues

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator._

  var source: SortedKeyValueIterator[Key, Value] = _

  var filter: Z2Filter = _
  var zOffset: Int = -1

  var topKey: Key = _
  var topValue: Value = _
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    import scala.collection.JavaConverters._

    this.source = source

    zOffset = options.get(Config.ZOffsetKey).toInt
    filter = Z2Filter.deserializeFromStrings(options.asScala)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  private def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop) {
      if (inBounds(source.getTopKey)) {
        topKey = source.getTopKey
        topValue = source.getTopValue
        return
      } else {
        source.next()
      }
    }
  }

  private def inBounds(k: Key): Boolean = {
    k.getRow(row)
    filter.inBounds(row.getBytes, zOffset)
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
    val iter = new Z2Iterator
    val opts = Z2Filter.serializeToStrings(filter) + (Config.ZOffsetKey -> zOffset.toString)
    iter.init(source.deepCopy(env), opts, env)
    iter
  }
}

object Z2Iterator {

  object Config {
    val ZOffsetKey = "zo"
  }

  def configure(values: Z2IndexValues, isSharing: Boolean, priority: Int): IteratorSetting = {
    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])
    // index space values for comparing in the iterator
    Z2Filter.serializeToStrings(Z2Filter(values)).foreach { case (k, v) => is.addOption(k, v) }
    // account for shard and table sharing bytes
    is.addOption(Config.ZOffsetKey, if (isSharing) { "2" } else { "1" })
    is
  }
}
